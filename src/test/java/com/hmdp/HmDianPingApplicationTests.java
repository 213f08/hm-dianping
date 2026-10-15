package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Shop;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Slf4j
@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private CacheClient cacheClient;

    @Resource
    private IShopService shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    /**
     * 缓存预热（逻辑过期时间较长，用于验证「未过期」路径）
     *
     * 逻辑过期方案的前提 —— 缓存中必须【永远有数据】：
     *   CacheClient.queryWithLogicalExpire 中，StrUtil.isBlank(json) 会直接 return null，
     *   并不会去查数据库。所以若不预热，接口会一直返回「店铺不存在」。
     *
     * 与 TTL 的区别：
     *   这里写入的 key【不设置 Redis 过期时间】（TTL = -1，永不过期），
     *   过期时间存在 value 内部的 expireTime 字段里，由代码自行判断。
     */
    @Test
    void testWarmUpFresh() {
        warmUp(1L, 300L);
    }

    /**
     * 缓存预热（逻辑过期时间极短，用于验证「已过期」路径）
     *
     * 2 秒后逻辑过期，此时请求应当：
     *   1. 立刻返回【旧数据】（不阻塞等待）
     *   2. 由抢到锁的线程【异步】重建缓存
     */
    @Test
    void testWarmUpExpired() {
        warmUp(1L, 2L);
    }

    /**
     * 通用预热方法
     *
     * @param id            商铺 id
     * @param expireSeconds 逻辑过期秒数
     */
    private void warmUp(Long id, Long expireSeconds) {
        Shop shop = shopService.getById(id);
        log.info("预热商铺：id={}, name={}", id, shop.getName());
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + id, shop, expireSeconds, TimeUnit.SECONDS);
        log.info("预热完成：key={}，逻辑过期时间={} 秒后（Redis TTL 应为 -1，即永不过期）",
                CACHE_SHOP_KEY + id, expireSeconds);
    }

    /**
     * 全局唯一 ID 压测：300 线程 × 100 次 = 30000 个 ID
     *
     * 验证三件事：
     *   1. 【唯一性】30000 个 ID 无重复 —— 这是全局 ID 生成器的底线
     *   2. 【递增性】ID 随时间趋势递增（时间戳部分）
     *   3. 【吞吐量】记录总耗时
     *
     * 用 CountDownLatch 做「发令枪」：让 300 个线程尽可能同时开跑，
     * 制造最大并发冲突，比顺序生成更容易暴露重复问题。
     */
    @Test
    void testIdWorker() throws InterruptedException {
        int threadCount = 300;
        int perThread = 100;

        // Set 用于去重检查；ConcurrentHashMap.newKeySet 是线程安全的 Set
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        // 另外单独记录一份（保留全部，用于统计总数）
        List<Long> allIds = Collections.synchronizedList(new ArrayList<>());

        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);   // 所有线程就绪
        CountDownLatch startLatch = new CountDownLatch(1);             // 发令枪
        CountDownLatch doneLatch = new CountDownLatch(threadCount);    // 全部完成

        for (int i = 0; i < threadCount; i++) {
            es.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();          // 等发令枪响，300 线程同时开跑
                    for (int j = 0; j < perThread; j++) {
                        long id = redisIdWorker.nextId("order");
                        ids.add(id);
                        allIds.add(id);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();                      // 等所有线程就绪
        long begin = System.currentTimeMillis();
        startLatch.countDown();                  // 发令！
        doneLatch.await();                       // 等全部完成
        long end = System.currentTimeMillis();
        es.shutdown();

        int total = threadCount * perThread;
        log.info("=== 全局唯一 ID 压测 ===");
        log.info("线程数={}, 每线程={}, 总计={}", threadCount, perThread, total);
        log.info("总耗时={} ms, 吞吐≈{} 个/秒", (end - begin),
                (long) (total * 1000.0 / Math.max(1, end - begin)));
        log.info("实际生成={} 个, 去重后={} 个", allIds.size(), ids.size());

        // 断言 1：唯一性
        org.junit.jupiter.api.Assertions.assertEquals(total, ids.size(),
                "ID 出现重复！生成 " + total + " 个但去重后只有 " + ids.size() + " 个");

        // 断言 2：结构反解 —— 高 32 位应当约等于「从 2022-01-01 至今的秒数」
        long sampleId = allIds.get(0);
        long extractedTs = sampleId >>> 32;              // 取高 32 位 = 时间戳
        long extractedCount = sampleId & 0xFFFFFFFFL;    // 取低 32 位 = 序列号
        long expectedTs = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - 1640995200L;
        log.info("样本 ID = {}", sampleId);
        log.info("  反解时间戳 = {} (期望约 {})", extractedTs, expectedTs);
        log.info("  反解序列号 = {}", extractedCount);

        org.junit.jupiter.api.Assertions.assertTrue(
                Math.abs(extractedTs - expectedTs) <= 5,
                "反解出的时间戳与当前时间偏差过大：" + extractedTs + " vs " + expectedTs);

        log.info("✅ 唯一性通过：{} 个 ID 无重复", total);
    }

    // ==================== 3.5 库存超卖复现 ====================

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /** 测试用秒杀券 id（数据库 tb_seckill_voucher 中 stock=100 的那张） */
    private static final Long VOUCHER_ID = 10L;

    /**
     * 【3.5 超卖复现】用真并发打 Service 层，看库存会不会被扣成负数
     *
     * 为什么并发数（200）必须【大于】库存（100）？
     *   超卖的本质是「读到旧值 → 再写」的竞态。如果线程数 ≤ 库存，
     *   理论上每个线程都能成功，即使实现有 bug 也看不出负数。
     *   只有「抢的人比货多」，失败者才会把库存扣穿，暴露 stock < 0。
     */
    @Test
    void testSeckillOversell() throws InterruptedException {
        int threadCount = 200;   // 抢购人数：必须 > 库存 100

        // ---- 记录每个线程的调用结果 ----
        AtomicInteger successCount = new AtomicInteger();   // 下单成功的线程数
        AtomicInteger failCount = new AtomicInteger();      // 被拒绝的线程数
        // 保存失败原因（用 Map 统计每种 errorMsg 出现了几次）
        Map<String, AtomicInteger> failReasons = new ConcurrentHashMap<>();

        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);   // 发令枪
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            // 每个线程分配一个【不同的】userId，模拟 200 个不同用户来抢
            final long userId = 1000L + i;
            es.submit(() -> {
                try {
                    // 关键：UserHolder 是 ThreadLocal，测试线程不走登录拦截器，
                    // 必须【手动塞用户】，否则 seckillVoucher 里 UserHolder.getUser() 返回 null → NPE
                    UserDTO user = new UserDTO();
                    user.setId(userId);
                    user.setNickName("test-user-" + userId);
                    UserHolder.saveUser(user);

                    readyLatch.countDown();
                    startLatch.await();      // 等发令枪，让 200 个线程尽量同时冲

                    Result result = voucherOrderService.seckillVoucher(VOUCHER_ID);

                    if (Boolean.TRUE.equals(result.getSuccess())) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                        failReasons.computeIfAbsent(result.getErrorMsg(),
                                k -> new AtomicInteger()).incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    failReasons.computeIfAbsent("异常:" + e.getClass().getSimpleName(),
                            k -> new AtomicInteger()).incrementAndGet();
                } finally {
                    UserHolder.removeUser();   // 必须清理，避免线程池复用污染
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        long begin = System.currentTimeMillis();
        startLatch.countDown();              // 🔫 发令！
        doneLatch.await();
        long end = System.currentTimeMillis();
        es.shutdown();

        // ---- 查数据库最终状态：这才是铁证 ----
        SeckillVoucher voucher = seckillVoucherService.getById(VOUCHER_ID);
        long orderCount = voucherOrderService.count(
                new QueryWrapper<com.hmdp.entity.VoucherOrder>().eq("voucher_id", VOUCHER_ID));

        log.info("=== 3.5 超卖复现结果 ===");
        log.info("抢购人数={}, 耗时={} ms", threadCount, (end - begin));
        log.info("下单成功={}, 被拒绝={}", successCount.get(), failCount.get());
        log.info("失败原因分布={}", failReasons);
        log.info("--------------------------------");
        log.info("数据库剩余库存 stock = {}", voucher.getStock());
        log.info("实际生成订单数 = {}", orderCount);

        // 理论正确值：库存 100，只能成功 100 单，剩余 0
        log.info("期望值：成功=100, 库存=0, 订单=100");
        if (voucher.getStock() < 0) {
            log.error("❌ 出现超卖！库存被扣成 {}，说明有 {} 件商品被超卖",
                    voucher.getStock(), -voucher.getStock());
        } else {
            log.info("✅ 未超卖");
        }
        if (successCount.get() > 100) {
            log.error("❌ 超卖确认：成功下单 {} 次 > 库存 100 件", successCount.get());
        }
    }

    // ==================== 3.6 一人一单 验证 ====================

    /**
     * 【3.6 一人一单】同一个用户，用 50 个线程同时抢同一张券
     *
     * 预期：只有 1 单成功，其余 49 次被「用户已经购买过一次！」拒绝。
     *
     * 为什么这个测试能测出问题？
     *   如果没有加锁，50 个线程会同时执行 count 查询，
     *   都查到 0（因为都还没插入），然后全部去 insert
     *   → 同一个用户产生多张订单。
     *
     * 注意：这里 50 个线程用的都是【同一个 userId】，
     *   这正是 synchronized(userId.toString().intern()) 要串行化的场景。
     */
    @Test
    void testOneOrderPerUser() throws InterruptedException {
        int threadCount = 50;
        final long sameUserId = 2001L;    // 同一个用户

        AtomicInteger successCount = new AtomicInteger();
        Map<String, AtomicInteger> failReasons = new ConcurrentHashMap<>();

        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            es.submit(() -> {
                try {
                    UserDTO user = new UserDTO();
                    user.setId(sameUserId);
                    user.setNickName("same-user");
                    UserHolder.saveUser(user);

                    readyLatch.countDown();
                    startLatch.await();

                    Result result = voucherOrderService.seckillVoucher(VOUCHER_ID);
                    if (Boolean.TRUE.equals(result.getSuccess())) {
                        successCount.incrementAndGet();
                    } else {
                        failReasons.computeIfAbsent(result.getErrorMsg(),
                                k -> new AtomicInteger()).incrementAndGet();
                    }
                } catch (Exception e) {
                    failReasons.computeIfAbsent("异常:" + e.getClass().getSimpleName(),
                            k -> new AtomicInteger()).incrementAndGet();
                } finally {
                    UserHolder.removeUser();
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        doneLatch.await();
        es.shutdown();

        // 查这个用户实际产生了几张订单
        long ordersOfThisUser = voucherOrderService.count(
                new QueryWrapper<com.hmdp.entity.VoucherOrder>()
                        .eq("user_id", sameUserId).eq("voucher_id", VOUCHER_ID));

        log.info("=== 3.6 一人一单验证 ===");
        log.info("线程数={}（同一用户 id={}）", threadCount, sameUserId);
        log.info("下单成功={}, 被拒绝={}", successCount.get(), 50 - successCount.get());
        log.info("失败原因分布={}", failReasons);
        log.info("--------------------------------");
        log.info("该用户在库中的订单数 = {}", ordersOfThisUser);
        log.info("期望：成功=1, 订单数=1");

        if (ordersOfThisUser == 1) {
            log.info("✅ 一人一单通过：同一用户只生成 1 张订单");
        } else {
            log.error("❌ 一人一单失败：同一用户生成了 {} 张订单！", ordersOfThisUser);
        }
    }

    // ==================== 第 5 章 Redisson ====================

    @Resource
    private RedissonClient redissonClient;

    /**
     * 【5.2 快速入门】Redisson 可重入锁基本用法
     */
    @Test
    void testRedisson() throws InterruptedException {
        // 1. 获取锁对象（此时还没有加锁，只是拿到一个 RLock 句柄）
        RLock lock = redissonClient.getLock("lock:test");

        // 2. tryLock(等待时间, 锁自动释放时间, 单位)：等待时间内会重试
        boolean isLock = lock.tryLock(1, 10, TimeUnit.SECONDS);
        log.info("=== Redisson 加锁 ===");
        log.info("tryLock 结果 = {}", isLock);

        if (isLock) {
            try {
                log.info("已持有锁，isLocked() = {}, isHeldByCurrentThread() = {}",
                        lock.isLocked(), lock.isHeldByCurrentThread());
                log.info("锁剩余时间 = {} ms", lock.remainTimeToLive());
            } finally {
                lock.unlock();
                log.info("已释放锁，isLocked() = {}", lock.isLocked());
            }
        }
    }

    /**
     * 【5.4 看门狗验证】不指定释放时间时，Redisson 会自动续期
     *
     * 默认锁有效期 30s，看门狗每 10s（1/3）续期一次，使锁一直存活到业务结束。
     * 这里只持有 25 秒并观察 TTL 变化，验证"未到期就被续上"。
     */
    @Test
    void testRedissonWatchDog() throws InterruptedException {
        RLock lock = redissonClient.getLock("lock:dog");
        // 只传等待时间、不传 leaseTime → 触发看门狗自动续期
        lock.tryLock(1, TimeUnit.SECONDS);
        try {
            log.info("=== 看门狗验证（默认 30s 有效期，每 10s 续期）===");
            for (int i = 1; i <= 3; i++) {
                log.info("第 {} 次观察：锁 TTL = {} ms", i, lock.remainTimeToLive());
                Thread.sleep(10000);
            }
            log.info("第 4 次观察（已过 30s）：锁 TTL = {} ms ← 若仍 > 0 说明被续期了",
                    lock.remainTimeToLive());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 【5.4 对照实验】显式指定 leaseTime 时，看门狗【不生效】
     *
     * 与 testRedissonWatchDog 的唯一区别：tryLock 传了 leaseTime=10s。
     * 预期：10 秒后锁自动消失，即使业务还在跑 —— 这正是自研简单锁的隐患。
     * 结论：只有【不传 leaseTime】才会启用看门狗自动续期。
     */
    @Test
    void testRedissonNoWatchDog() throws InterruptedException {
        RLock lock = redissonClient.getLock("lock:dog2");
        // 3 个参数：等待 1 秒、显式租约 10 秒（指定后看门狗不介入）
        lock.tryLock(1, 10, TimeUnit.SECONDS);
        try {
            log.info("=== 对照实验：显式 leaseTime=10s，看门狗不生效 ===");
            log.info("刚加锁：TTL = {} ms, isLocked = {}", lock.remainTimeToLive(), lock.isLocked());
            Thread.sleep(12000);
            log.info("12 秒后：TTL = {} ms, isLocked = {} ← 锁已过期消失",
                    lock.remainTimeToLive(), lock.isLocked());
        } finally {
            // 锁可能已自动过期，此时 unlock 会抛异常，故先判断
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
            log.info("已结束对照实验");
        }
    }
}

