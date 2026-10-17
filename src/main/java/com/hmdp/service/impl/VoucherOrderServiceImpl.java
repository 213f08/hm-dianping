package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 订单服务：第 6 章的异步秒杀实现
 * 入口线程：只用 Redis 做资格判断（快），判断通过就返回，不等数据库
 * 后台线程：从阻塞队列取订单，慢慢落库（慢）
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** 秒杀资格判断的 Lua 脚本（库存 + 一人一单，原子） */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /** 阻塞队列：存放 Redis 判断通过、待落库的订单 */
    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    /** 单线程池：一个后台线程慢慢落库，避免多线程争抢数据库 */
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /** 代理对象：后台线程里 this 调用会让 @Transactional 失效，必须用代理 */
    private IVoucherOrderService proxy;

    /** 启动后台消费线程（代理在 seckillVoucher 里取，这里取会抛 Cannot find current proxy） */
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 秒杀下单入口：只做 Redis 快速判断，不落库
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 取用户和生成订单 id
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 2. 执行 Lua：库存判断 + 一人一单 + 扣库存 + 记录用户，全部原子完成
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        // 3. 判断返回值：0=成功，1=库存不足，2=重复下单
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 4. 组装订单放进阻塞队列，交给后台线程落库
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        // 取代理对象（此刻处于代理调用链中，ThreadLocal 里才有 proxy）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        orderTasks.add(voucherOrder);

        // 5. 立刻返回订单 id，不等落库
        return Result.ok(orderId);
    }

    /**
     * 真正下单（一个事务内完成）：由后台线程通过代理调用
     * 事务覆盖"扣库存 + 建订单"，任一失败则整体回滚
     */
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 一人一单兜底：Redis 可能丢数据，数据库再查一次
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            throw new RuntimeException("用户已经购买过一次！");
        }

        // 2. 扣库存：WHERE 带 stock > 0，由数据库在行锁内完成"判断+扣减"，防超卖
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            throw new RuntimeException("库存不足！");
        }

        // 3. 保存订单
        save(voucherOrder);
    }

    /** 后台消费线程：死循环从队列取订单 → 落库 */
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 取订单（队列空时阻塞等待）
                    VoucherOrder order = orderTasks.take();
                    // 2. 加锁后落库
                    handleVoucherOrder(order);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }

        /** 落库：同一用户加 Redisson 锁串行，再用代理调用事务方法 */
        private void handleVoucherOrder(VoucherOrder order) {
            Long userId = order.getUserId();
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            // 1. 抢锁，失败说明该用户已有订单在处理
            boolean isLock = lock.tryLock();
            if (!isLock) {
                log.error("不允许重复下单");
                return;
            }
            try {
                // 2. 用代理调用，事务才生效
                proxy.createVoucherOrder(order);
            } finally {
                // 3. 保证释放锁
                lock.unlock();
            }
        }
    }
}
