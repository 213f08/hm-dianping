package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static com.hmdp.utils.RedisConstants.*;

/**
 * 缓存工具类：封装三种查询策略
 * queryWithPassThrough —— 缓存穿透（空值缓存）
 * queryWithMutex        —— 缓存击穿（互斥锁，强一致但阻塞）
 * queryWithLogicalExpire —— 缓存击穿（逻辑过期，高可用但有脏数据）
 */
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    /** 缓存重建线程池（逻辑过期方案异步重建用） */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /** 写入缓存并设置 TTL */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /** 写入缓存但不设 TTL，过期时间存 value 内部（逻辑过期） */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 1. 包装成 RedisData（内含 expireTime）
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 2. 写入 Redis，不设 TTL（永不过期）
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透方案：查不到就写空值，防止反复打库
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;

        // 1. 查缓存，命中非空值直接返回
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        // 2. 命中空字符串，说明之前查过且数据库也不存在，直接返回 null
        if (json != null) {
            return null;
        }

        // 3. 查数据库
        R r = dbFallback.apply(id);
        // 4. 数据库也没有 → 写空值缓存（短 TTL），防止穿透
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 5. 数据库有 → 写入缓存并返回
        this.set(key, r, time, unit);
        return r;
    }

    /**
     * 缓存击穿方案二：逻辑过期
     * 过期后仍返回旧数据，由抢到锁的线程异步重建（高可用，但短暂脏数据）
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        // 1. 查缓存，没有就直接返回（该方案依赖预热）
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 2. 反序列化出数据和逻辑过期时间
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 3. 未过期，直接返回
        if(expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }

        // 4. 已过期，尝试抢锁重建（锁 key 由 keyPrefix 派生，避免跨业务串锁）
        String lockKey = LOCK_KEY_PREFIX + keyPrefix + id;
        boolean isLock = tryLock(lockKey);
        if (isLock){
            // 5. double check：再读一次，可能已被其他线程重建好（实测不做会重复查库）
            String latestJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(latestJson)) {
                RedisData latestData = JSONUtil.toBean(latestJson, RedisData.class);
                if (latestData.getExpireTime().isAfter(LocalDateTime.now())) {
                    unlock(lockKey);
                    return JSONUtil.toBean((JSONObject) latestData.getData(), type);
                }
            }
            // 6. 确认仍过期 → 开异步线程重建，主线程先返回旧数据
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R newR = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }
            });
        }
        // 7. 无论是否抢到锁，都先返回旧数据（不阻塞）
        return r;
    }

    /**
     * 缓存击穿方案一：互斥锁
     * 没抢到锁的线程休眠后递归重试（强一致，但并发时阻塞）
     */
    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        // 1. 查缓存，命中非空值返回
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, type);
        }
        // 2. 命中空值，直接返回 null
        if (shopJson != null) {
            return null;
        }

        // 3. 抢锁（锁 key 由 keyPrefix 派生）
        String lockKey = LOCK_KEY_PREFIX + keyPrefix + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4. 没抢到锁 → 休眠 50ms 后递归重试（重新查缓存，实现 double check）
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            // 5. 抢到锁 → 查库
            r = dbFallback.apply(id);
            if (r == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6. 写缓存
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            // 7. 释放锁
            unlock(lockKey);
        }
        return r;
    }

    /** 加锁：SETNX + TTL，防死锁 */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /** 解锁：直接删除 */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
