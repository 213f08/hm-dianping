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
     * 秒杀下单入口：只做前置校验（不涉及并发写），加锁后交给 createVoucherOrder 完成下单
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 查优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在！");
        }
        // 2. 判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始！");
        }
        // 3. 判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }
        // 4. 判断库存（只做快速失败，拦不住并发：读的是 getById 的旧快照）
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足！");
        }

        // 5. 取当前用户，并用「用户级」的 Redis 分布式锁替代 synchronized（可跨 JVM）
        Long userId = UserHolder.getUser().getId();
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);

        // 6. 抢锁；抢不到说明该用户已有请求在处理，直接拒绝（1200 为锁的自动过期秒数）
        boolean isLocked = lock.tryLock(1200);
        if (!isLocked) {
            return Result.fail("请勿重复下单！");
        }
        try {
            // 7. 通过代理对象调用，@Transactional 才生效（this 调用不走代理）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 8. 在 finally 解锁：保证异常时也释放；且解锁在事务提交之后
            lock.unlock();
        }
    }

    /**
     * 真正下单（一个事务内完成）
     *
     * 单独抽成方法是为了让锁包住整个事务：加锁 → 开事务 → 业务 → 提交 → 解锁。
     * 若锁写在方法内部，会先解锁再提交事务，后到的线程读不到未提交数据 → 仍会重复下单。
     */
    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 1. 一人一单：查该用户对这张券是否已下过单
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过一次！");
        }

        // 2. 扣减库存：WHERE 带 stock > 0，由数据库在行锁内完成"判断+扣减"，防超卖
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("库存不足！");
        }

        // 3. 生成全局唯一订单号并保存订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }
}
