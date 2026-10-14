package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
}

