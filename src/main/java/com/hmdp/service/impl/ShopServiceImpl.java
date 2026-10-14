package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

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

    private final StringRedisTemplate stringRedisTemplate;

    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 尝试获取互斥锁（本质是 Redis 的 SETNX）
     *
     * setIfAbsent(key, value, timeout, unit) 等价于 SET key value NX EX timeout，是原子操作：
     *   key 不存在 → 设置成功返回 true  → 抢到锁
     *   key 已存在 → 设置失败返回 false → 锁被别人持有
     *
     * 第三个参数（过期时间）是【防死锁】的关键：
     *   如果持锁线程在 unlock 之前崩溃/被 kill，它无法执行释放动作，
     *   但锁会在这 10 秒后自动过期，其他线程依然能继续抢锁。
     *   没有这个过期时间 → 锁永远留在 Redis → 死锁。
     *
     * 用 BooleanUtil.isTrue(flag) 而不是直接 return flag：
     *   setIfAbsent 返回的是包装类型 Boolean（可能为 null），
     *   直接 return 会触发自动拆箱 → NullPointerException。
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放互斥锁：直接把锁的 key 删掉
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
    /**
     * 入口方法：调用具体方案，并把 Shop 包装成 Result
     */
    @Override
    public Result queryById(Long id) {
        // TODO 后续 3.0/3.1 会在这里切换不同方案（穿透 / 互斥锁 / 逻辑过期）
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    /**
     * 互斥锁方案：只让一个线程去重建缓存，其余线程休眠重试（double check）
     *
     * 解决的是【缓存击穿】：热点 key 突然失效的瞬间，大量并发请求同时涌向数据库。
     * 加锁后只放一个线程去查库重建，其他线程排队等它重建完再读缓存。
     *
     * 查不到统一返回 null，由 queryById 包装成 Result.fail
     *
     * @param id 商铺 id
     * @return 商铺对象；不存在则返回 null
     */
    private Shop queryWithMutex(Long id) {
//        1. 从 redis 查询商铺缓存
        String key=CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        2. 判断是否命中真实数据
        if (StrUtil.isNotBlank(shopJson)){
            // 命中，反序列化后直接返回，不需要抢锁、也不查库
            return JSONUtil.toBean(shopJson, Shop.class);
        } else if (shopJson != null) {
            // 命中的是空值标记（""）→ 说明之前查过且数据库确实没有 → 直接返回 null，不查库（防穿透）
            return null;
        }
//        3. 缓存未命中 → 进入缓存重建流程，先抢互斥锁
//           注意：每个商铺一把锁（key 带上 id），互不干扰
        String lockkey=LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockkey);
            // 4. 判断是否抢到锁
            if (!isLock) {
                // 没抢到 → 说明已有线程在重建缓存 → 休眠一会儿，然后【递归重试整个方法】
                //   递归的意义：回到方法开头【再查一次 Redis】（double check）
                //   等锁释放时缓存通常已被别人建好，这次就能直接命中，不必查库
                Thread.sleep(100);
                return queryWithMutex(id);
            }
//            5. 抢到锁 → 由本线程独占执行数据库查询与缓存重建
            shop = getById(id);
            if (shop == null) {
                // 数据库也没有 → 写入空值标记（短 TTL），防止后续请求继续穿透到数据库
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
//            6. 查到数据 → 写入缓存并设置过期时间，供后续请求直接命中
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch (Exception e){
            // 查库/序列化等异常统一包装抛出，交给全局异常处理器
            throw new RuntimeException(e);
        }finally {
            // 7. 释放锁：必须放在 finally，保证即使抛异常也能解锁，否则其他线程永远抢不到
            unlock(lockkey);
        }
        return shop;
    }

    @Override
    public Result updateShop(Shop shop) {
//        先校验id是否存在
        Long shopId = shop.getId();
        if (shopId == null) {
            return Result.fail("店铺id不能为空");
        }
        // 更新数据库
        updateById(shop);
        //删除redis的库，等待下一次查缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
        return Result.ok();
    }
}
