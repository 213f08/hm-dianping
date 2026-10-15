package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的分布式锁实现
 *
 * 【为什么需要它？】
 *   3.7 实测证明：synchronized 只在单个 JVM 内有效。集群部署（8081/8082）时，
 *   两个 JVM 各有一把自己的锁，同一用户会在两台机器上各下一单（实测 30 个用户重复 27 个）。
 *   把锁放到 Redis 里，所有实例操作同一个 key，就能共享同一把锁。
 *
 * 【演进过程（每一步都在补前一步的漏）】
 *   4.3  SETNX + EX 加锁，DEL 释放
 *         漏洞：锁超时自动释放后，原持有者恢复执行，会 DEL 掉【新持有者】的锁
 *   4.5  存"线程标识"，删锁前先比对 GET == 自己 ?
 *         漏洞：GET 与 DEL 是两条命令，中间锁可能过期 → 比对通过仍会删错锁
 *   4.7  用 Lua 把"比对 + 删除"焊成一步（Redis 执行脚本期间不插入其他命令）
 *         本类即为最终版，同时包含以上全部修补。
 */
public class SimpleRedisLock implements ILock {

    /** 锁 key 的统一前缀，便于在 Redis 中识别与排查（KEYS lock:*） */
    private static final String KEY_PREFIX = "lock:";

    /**
     * 线程标识前缀：每个 JVM 启动时随机生成一次（static final 只执行一次）
     *
     * 【为什么必须加这个 UUID？】
     *   Thread.currentThread().getId() 只在【单个 JVM 内】唯一。
     *   JVM A 的线程 5 和 JVM B 的线程 5 会算出相同的标识 "5"，
     *   导致 B 的线程误以为 A 的锁是自己的，从而误删。
     *   加上每实例唯一的 UUID 前缀后：
     *     JVM A → "a1b2c3-5"，JVM B → "d4e5f6-5" —— 永不冲突
     */
    private static final String ID_PREFIX = UUID.randomUUID().toString() + "-";

    /**
     * 释放锁的 Lua 脚本载体
     *
     *   DefaultRedisScript 只是【脚本的容器】：负责读取 unlock.lua 并缓存。
     *   它本身不执行任何命令 —— 真正的执行发生在 unlock() 里的 execute(...)。
     */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    /** 业务名，如 "order:1"，最终拼成锁 key：lock:order:1 */
    private String name;

    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        // 从 classpath 根目录读取（即 src/main/resources/unlock.lua）
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        // 脚本 return 的是 0 / 1，声明为 Long 便于接收
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 尝试获取锁（非阻塞：抢不到【立刻】返回 false，不等待）
     *
     * @param timeoutSec 锁的自动过期时间（秒），防止持有者崩溃导致死锁
     * @return true=抢到锁, false=没抢到
     */
    @Override
    public boolean tryLock(long timeoutSec) {
        // 1. 算出"我是谁"：JVM 唯一前缀 + 线程 id（unlock 里必须用同样的算法）
        String threadId = ID_PREFIX + Thread.currentThread().getId();

        // 2. 抢锁 —— 这一行等价于 Redis 命令：
        //      SET lock:order:1 <threadId> NX EX 1200
        //        key=锁名   value=持有者  NX=不存在才写入  EX=过期时间
        //    NX 是互斥的关键：key 已存在则写入失败（返回 nil → Java 的 null）
        //    一条命令同时完成"判断不存在 + 写入 + 设过期"，因此是原子的；
        //    若拆成 setnx() + expire() 两条，中间崩溃就会留下永久死锁。
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);

        // 3. 注意：抢锁失败时返回的是 null（不是 false），必须用 TRUE.equals 避免拆箱 NPE
        return Boolean.TRUE.equals(success);
    }

    /**
     * 释放锁（只释放【自己的】锁）
     *
     * 为什么不直接 delete？
     *   因为 DEL 只认 key、不认持有者。若本线程的锁已超时自动释放、
     *   而另一个线程刚抢到锁，此时无条件 DEL 会把【别人的锁】删掉，
     *   导致两人同时进入临界区。
     *
     * 为什么用 Lua 而不是 Java 里先 GET 再 DEL？
     *   两者是两条独立命令，中间存在时间缝：GET 比对通过后锁可能刚好过期，
     *   等到 DEL 执行时删的还是别人的锁（4.6 的原子性问题）。
     *   Lua 脚本在 Redis 内【一次性执行完毕】，中间不插入任何其他命令。
     */
    @Override
    public void unlock() {
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                // 第 2 个参数是 KEYS 列表（Redis 支持多个 key，这里只有一个）
                // → 脚本里通过 KEYS[1] 取到（注意 Lua 数组从 1 开始）
                Collections.singletonList(KEY_PREFIX + name),
                // 第 3 个起是 ARGV 可变参数 → 脚本里通过 ARGV[1] 取到
                // 算法必须与 tryLock 完全一致，否则比对永远失败、锁永远删不掉
                ID_PREFIX + Thread.currentThread().getId());
    }
}