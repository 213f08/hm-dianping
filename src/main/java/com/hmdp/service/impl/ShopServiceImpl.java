package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private CacheClient cacheClient;

    private final StringRedisTemplate stringRedisTemplate;

    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 入口方法：选择具体方案查询商铺，并把结果包装成 Result
     *
     * 三套方案的具体实现已下沉到 CacheClient，此处只负责选型与结果包装：
     *   1. queryWithPassThrough   —— 缓存空值，解决缓存穿透
     *   2. queryWithMutex         —— 互斥锁，解决缓存击穿（一致性优先，但会串行等待）
     *   3. queryWithLogicalExpire —— 逻辑过期，解决缓存击穿（性能优先，但会短暂返回旧数据）
     *
     * 注意：逻辑过期方案依赖缓存预热（CacheClient.setWithLogicalExpire），
     *      若缓存中没有数据会直接返回 null，不会去查数据库。
     */
    @Override
    public Result queryById(Long id) {
        // 方案一：解决缓存穿透（默认启用 —— 不依赖预热，最稳妥）
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 方案二：互斥锁解决缓存击穿（一致性优先，但并发时会串行等待）
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 方案三：逻辑过期解决缓存击穿（性能优先，但会短暂返回旧数据；需先预热缓存）
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    /**
     * 更新商铺：先改数据库，再删除缓存（Cache Aside）
     *
     * 顺序不能反 —— 先删缓存会因并发导致旧值被写回，新数据被旧数据覆盖。
     */
    @Override
    public Result updateShop(Shop shop) {
        // 先校验 id
        Long shopId = shop.getId();
        if (shopId == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1. 更新数据库
        updateById(shop);
        // 2. 删除缓存，等下一次查询时自然重建为新值
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
        return Result.ok();
    }
}
