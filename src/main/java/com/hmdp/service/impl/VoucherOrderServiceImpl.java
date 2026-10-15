package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
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

    @Resource
    private StringRedisTemplate stringRedisTemplate;        // 分布式锁用

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

        // ★ 用 Redis 分布式锁替代 synchronized ★
        //
        // 【为什么换掉 synchronized？】3.7 实测：synchronized 只在单个 JVM 内有效。
        //   集群部署时，8081 和 8082 各有一把锁 → 同用户在两台机器各下一单
        //   （实测 30 个用户中有 27 个重复下单）。
        //   Redis 锁的 key 在【所有实例共享】，因此能真正跨 JVM 互斥。
        //   （换锁后的实测：同样 30 个用户并发打两个实例，零重复。）
        //
        // 【锁的 key】"order:" + userId → 锁粒度精确到「用户」，
        //   不同用户之间互不阻塞；同一个用户的请求才串行化。
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);

        // tryLock 是【非阻塞】的：抢不到立刻返回 false（不等待）。
        // 参数 1200 是锁的自动过期时间，单位由 SimpleRedisLock 内部定为【秒】
        //   （setIfAbsent 的第 4 个参数是 TimeUnit.SECONDS）→ 即 1200 秒。
        // 作用：持有者崩溃/卡死时锁能自动释放，避免死锁。
        // ⚠️ 隐患（第 5 章 Redisson 要解决）：若业务执行时间超过 1200 秒，
        //    锁会在业务完成前过期 → 别的线程拿到锁 → 互斥被破坏。
        //    Redisson 的看门狗机制会给未完成的业务自动续期。
        boolean isLocked = lock.tryLock(1200);
        if (!isLocked) {
            // 没抢到锁 → 说明该用户已有请求正在下单 → 直接拒绝
            return Result.fail("请勿重复下单！");
        }
        try {
            // 必须通过【代理对象】调用，@Transactional 才会生效：
            //   this.createVoucherOrder(...) 是同类内部调用，不经过代理 → 事务失效。
            //   AopContext.currentProxy() 取出 Spring 已创建好的代理对象。
            //   前提：启动类上有 @EnableAspectJAutoProxy(exposeProxy = true)。
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 必须在 finally 释放：否则业务抛异常时锁不释放，
            // 只能等 1200ms 超时，期间该用户的所有请求都会被拒绝。
            //
            // 锁的释放时机也正确：此处解锁发生在 proxy.createVoucherOrder()
            // 返回【之后】，即事务已提交之后 —— 保证"锁比事务活得久"，
            // 后到的线程一定能看到前一个已提交的订单。
            lock.unlock();
        }
    }

    /**
     * 真正下单：一人一单校验 + 扣库存 + 生成订单
     *
     * 【为什么拆成独立方法？】见 seckillVoucher() 中的说明：
     * 为了让「锁」能包住「整个事务」，所以把事务方法单独抽出来，
     * 由外层的分布式锁包住代理调用，保证顺序为：
     *   加锁 → 开事务 → 业务 → 提交事务 → 释放锁
     * 若把锁写在方法内部，顺序会变成「先释放锁、后提交事务」，
     * 后到的线程读不到未提交的数据 → 仍然会重复下单。
     */
    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 6.1 一人一单判断：查这个用户对这张券有没有下过单
        //     order 表里 user_id + voucher_id 的组合唯一
        Long userId = UserHolder.getUser().getId();

        //     【并发陷阱】"先查后插"同样是 check-then-act 竞态：
        //     两个线程同时查，都查到 count=0，然后都去插入 → 一个用户两单。
        //     拦住它的是外层那把 Redis 分布式锁 —— 同一用户的请求被串行化，
        //     后到的线程必然能看到前一个已提交的订单。
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过一次！");
        }

        // 6.2 扣减库存（乐观锁：把「库存判断」交给数据库）
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
