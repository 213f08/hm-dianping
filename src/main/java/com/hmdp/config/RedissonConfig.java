package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 客户端配置
 *
 * Redisson 是第 5 章的分布式锁实现，解决自研 SimpleRedisLock 的 4 个问题：
 *   1. 不可重入 —— 同一线程无法再次获取自己已持有的锁（Redisson 用 hash 结构记录重入次数）
 *   2. 不可重试 —— 抢不到只能立刻失败（Redisson 的 tryLock 支持等待时间 + 重试）
 *   3. 超时释放隐患 —— 业务没做完锁就过期（Redisson 看门狗自动续期）
 *   4. 主从一致性 —— 主节点宕机可能导致锁丢失（Redisson MultiLock 解决）
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        // 单机模式；本机 Redis 未设密码，故不调用 setPassword
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        return Redisson.create(config);
    }
}
