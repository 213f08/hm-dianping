package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Slf4j
@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private CacheClient cacheClient;

    @Resource
    private IShopService shopService;

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
}
