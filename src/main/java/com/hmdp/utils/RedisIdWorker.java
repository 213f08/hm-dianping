package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 全局唯一 ID 生成器
 * 结构：64 位 = 时间戳(高 32 位) | 序列号(低 32 位)
 * 相比 UUID：UUID 随机做主键会导致 InnoDB 页分裂；本方案趋势递增，插入性能好
 */
@Component
public class RedisIdWorker {

    /** 计秒起点 2022-01-01，减掉它可缩短 ID、延长可用年限 */
    private static final long BEGIN_TIMESTAMP = 1640995200L;

    /** 序列号占 32 位 */
    private static final int COUNT_BITS = 32;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 生成全局唯一 ID
     * @param keyPrefix 业务前缀（如 "order"），避免不同业务共用同一自增序列
     */
    public long nextId(String keyPrefix) {
        // 1. 时间戳 = 当前 UTC 秒数 - 起点（相对秒数，让数值变小）
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;

        // 2. 序列号：按天分 key 自增，每天从 1 重新计数，避免 32 位溢出
        String date = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3. 拼接：时间戳左移 32 位腾出低位，用 | 把序列号填进去（不用 + ，避免进位污染高位）
        return timeStamp << COUNT_BITS | count;
    }
}
