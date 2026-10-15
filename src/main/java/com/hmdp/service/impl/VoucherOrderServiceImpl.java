package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;   // 查券/扣库存

    @Resource
    private RedisIdWorker redisIdWorker;                    // 生成订单ID

    /**
     * 秒杀下单入口
     *
     * 【职责划分】本方法只做「不涉及并发写」的前置校验（查券、判时间、判库存快照），
     * 真正会并发修改数据的部分（一人一单判断 + 扣库存 + 建订单）全部下沉到
     * createVoucherOrder()，由它在一个事务 + 一把锁里完成。
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 查优惠券（用 seckillVoucherService.getById）
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在！");
        }
        // 2. 判断秒杀是否开始（beginTime.isAfter(now)）
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始！");
        }
        // 3. 判断秒杀是否结束（endTime.isBefore(now)）
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }
        // 4. 前置校验：库存是否充足（stock < 1）
        //    【注意】这一步【拦不住并发】：voucher 是上面 getById 查出的「快照」，
        //    200 个线程几乎同时读，都会看到同一个旧值（例如 100），于是全部通过。
        //    它的价值是「快速失败」——库存已为 0 时，让请求在这里就返回，
        //    避免它们再去执行 UPDATE 抢占同一行的行锁（减少数据库无用压力）。
        //    真正的并发安全，由 createVoucherOrder() 里的 SQL 保证。
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足！");
        }

        // 5. 一人一单：用户 id 从 ThreadLocal 取
        Long userId = UserHolder.getUser().getId();

        // ★★★ 加锁 + 事务代理：这两件事的顺序，是本小节的核心 ★★★
        //
        // 【为什么锁要加在这里，而不是 createVoucherOrder() 方法里面？】
        //   必须保证「锁的释放」发生在「事务提交之后」。
        //
        //   假设把 synchronized 写在 createVoucherOrder() 内部：
        //     @Transactional 的边界在方法【外层】（代理层开启事务）
        //     synchronized 在方法【内层】
        //     → 顺序变成：开事务 → 加锁 → 业务 → 释放锁 → 提交事务
        //     → 危险窗口：锁已释放但数据未提交，别的线程读到旧数据 → 重复下单
        //
        //   写在 seckillVoucher() 里、包住 proxy 调用，顺序就变成：
        //     加锁 → (代理)开事务 → 业务 → 提交事务 → 释放锁
        //     → 事务被锁完整包住，安全 ✅
        //
        // 【为什么要 intern()？】
        //   synchronized 锁的是「对象」。userId 是 Long，toString() 每次都 new 出
        //   一个新的 String 对象，两个线程拿到的是两个不同对象 → 锁不住！
        //   intern() 从字符串常量池返回同一个引用 → 相同 userId 拿到同一把锁 ✅
        //   好处：不同用户之间不互相阻塞，锁粒度精确到「用户」，而非整个方法。
        //
        // 【为什么要通过代理对象调用？】
        //   @Transactional 靠动态代理生效。this.createVoucherOrder() 是同类内部调用，
        //   不经过代理 → 事务注解失效（Spring 事务最经典的失效场景）。
        //   AopContext.currentProxy() 拿到代理对象，让调用重新走代理链路。
        //   前提：启动类必须加 @EnableAspectJAutoProxy(exposeProxy = true)，
        //        否则抛 IllegalStateException: Cannot find current proxy。
        //   强转说明：代理对象实现的是 IVoucherOrderService 接口，故转成接口类型。
        synchronized (userId.toString().intern()) {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    /**
     * 真正下单：一人一单校验 + 扣库存 + 生成订单
     *
     * 【为什么拆成独立方法？】见 seckillVoucher() 中的说明：
     * 为了让「锁」能包住「整个事务」，所以把事务方法单独抽出来，
     * 由外层的 synchronized 包住代理调用。
     *
     * ⚠️ 3.7 会指出：synchronized 只在【单机 JVM】内有效；
     *    集群部署（8081/8082 两个实例）时锁不住 → 需要第 4 章的 Redis 分布式锁。
     */
    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 6.1 一人一单判断：查这个用户对这张券有没有下过单
        //     order 表里 user_id + voucher_id 的组合唯一
        Long userId = UserHolder.getUser().getId();

        //     【并发陷阱】"先查后插"同样是 check-then-act 竞态：
        //     两个线程同时查，都查到 count=0，然后都去插入 → 一个用户两单。
        //     拦住它的是外层那把 synchronized(userId) —— 同一用户的请求被串行化，
        //     后到的线程必然能看到前一个已提交的订单。
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过一次！");
        }

        // 6.2 扣减库存（乐观锁：把「库存判断」交给数据库）
        //     ★ 与 3.5 超卖版的唯一区别：WHERE 里多了 stock > 0
        //
        //     3.5 的错误写法：.eq("voucher_id", voucherId).update()
        //       → WHERE 只有 voucher_id，该行永远存在，affected rows 恒为 1
        //       → success 恒为 true，下面的 if (!success) 是【死代码】
        //       → 200 线程各扣 1 → stock 被扣成 -100
        //
        //     现在改为：WHERE voucher_id = ? AND stock > 0
        //       → 数据库在【行锁保护下】完成「判断 + 扣减」这一个原子操作
        //       → 判断与扣减之间不再有时间缝，超卖从根上消失
        //       → stock 为 0 时匹配不到行，affected rows = 0，success = false
        //       → success 第一次真正携带了「库存不足」的语义
        //
        //     【链式顺序】.gt() 是条件构造器方法，必须在 .update() 之前；
        //     .update() 是终端方法，执行 SQL 并返回 boolean，调用后无法再链式加条件。
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)          // ← WHERE ... AND stock > 0（关键的一行）
                .update();
        if (!success) {
            return Result.fail("库存不足！");
        }

        // 6.3 创建订单（VoucherOrder + nextId("order") + save）
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }
}
