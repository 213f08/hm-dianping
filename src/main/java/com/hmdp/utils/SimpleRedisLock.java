package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的分布式锁
 * synchronized 只在单 JVM 内有效，集群下会失效（3.7 实测 30 用户重复 27 个）
 * 把锁放进 Redis，所有实例操作同一个 key，才能真正互斥
 */
public class SimpleRedisLock implements ILock {

    /** 锁 key 前缀 */
    private static final String KEY_PREFIX = "lock:";

    /** 线程标识前缀：每个 JVM 启动时生成一次，避免跨实例线程 id 冲突 */
    private static final String ID_PREFIX = UUID.randomUUID().toString() + "-";

    /** 释放锁的 Lua 脚本载体（只负责读文件，执行在 unlock 里） */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    /** 业务名，拼成锁 key：lock:order:1 */
    private String name;

    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 尝试获取锁（非阻塞：抢不到立刻返回 false）
     * @param timeoutSec 锁自动过期时间（秒），防止持有者崩溃导致死锁
     */
    @Override
    public boolean tryLock(long timeoutSec) {
        // 1. 算出"我是谁"：JVM 唯一前缀 + 线程 id（unlock 必须用同样算法）
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 2. 抢锁，等价 SET lock:order:1 <threadId> NX EX 1200
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        // 3. 失败返回 null（不是 false），用 TRUE.equals 避免拆箱 NPE
        return Boolean.TRUE.equals(success);
    }

    /** 释放锁：只删自己的锁（用 Lua 保证"比对 + 删除"原子，防止删掉别人的锁） */
    @Override
    public void unlock() {
        // 参数 2 是 KEYS（脚本里 KEYS[1]），参数 3 起是 ARGV（脚本里 ARGV[1]）
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }
}
