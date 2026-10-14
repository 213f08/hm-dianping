package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 全局唯一 ID 生成器（基于 Redis 自增）
 *
 * <p>订单表如果用数据库自增 ID 有两个问题：
 * <ol>
 *   <li>规律性太明显 —— 竞争对手从订单 id 就能推算出你一天卖了多少单</li>
 *   <li>受单表容量限制 —— MySQL 单表不宜超 500W，拆表后各表自增 id 会重复</li>
 * </ol>
 *
 * <p>本类的 ID 由两部分【拼接】而成（共 64 位）：
 * <pre>
 *   ┌─┬────────────────────────┬──────────────────────────┐
 *   │0│      时间戳 31bit       │      序列号 32bit         │
 *   └─┴────────────────────────┴──────────────────────────┘
 *    ↑            ↑                          ↑
 *  符号位   距今多少秒(相对起点)        这一秒内的第几单
 *  永远为0   可用约 69 年            每秒最多 2^32 个
 * </pre>
 *
 * <p>相比 UUID：UUID 完全随机，做主键会导致 InnoDB 频繁页分裂；
 * 而本方案是【趋势递增】的，新数据总是追加到 B+ 树末尾，插入性能好。
 *
 * @author 虎哥
 */
@Component
public class RedisIdWorker {

    /**
     * 开始时间戳（计秒起点），等于 2022-01-01 00:00:00 UTC 对应的秒数
     *
     * <p>作用：减掉它可以让时间戳变小，从而缩短 ID 长度、延长可用年限。
     * 若直接从 1970 年算起，现在已经过了 56 年，31 位很快就不够用了。
     */
    private static final long BEGIN_TIMESTAMP = 1640995200L;

    /**
     * 序列号占用的位数（低 32 位）
     */
    private static final int COUNT_BITS = 32;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 生成全局唯一 ID
     *
     * @param keyPrefix 业务前缀（如 "order"），用于区分不同业务的计数器，
     *                  避免订单、优惠券等业务的 ID 挤在同一个自增序列上
     * @return 全局唯一且趋势递增的 long 型 ID
     */
    public long nextId(String keyPrefix) {
        // 1. 生成时间戳部分
        //    now 是【当前时刻对象】（不是字符串，也还不是数字）
        LocalDateTime now = LocalDateTime.now();

        //    nowSecond = 从 1970-01-01（Unix 纪元）到现在，一共过了多少秒（绝对秒数）
        //    用 UTC 解释，与下面 BEGIN_TIMESTAMP 的口径保持一致
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);

        //    timeStamp = 从【本项目的起点 2022-01-01】到现在，间隔了多少秒（相对秒数）
        //    这一步的目的只是让数值变小，方便塞进 31 位
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;

        // 2. 生成序列号部分
        //    2.1 取当天日期字符串，如 "2026-10-14"，用于拼 key
        String date = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        //    2.2 按【天】维度做 Redis 自增，key 形如 icr:order:2026-10-14
        //        为什么按天分 key：序列号只有 32 位（上限约 42.9 亿），
        //        若只用一个 key 无限自增迟早会溢出并污染高位的时间戳、产生重复 ID；
        //        按天分 key 后每天从 1 重新计数，永远够用。
        //        keyPrefix 用于区分业务，避免不同业务互相干扰计数。
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3. 拼接并返回
        //    timeStamp << 32 ：把时间戳整体左移 32 位，让低 32 位空出来（填 0）
        //    | count        ：按位或，把序列号填进低 32 位
        //                     因为低 32 位原本全是 0，所以效果等于"填入"；
        //                     用 | 而不是 + 是为了避免进位污染高位。
        //
        //    结果布局：高 32 位 = timeStamp，低 32 位 = count
        //    需要反解时可分别取：id >>> 32 取时间戳， id & 0xFFFFFFFFL 取序列号
        return timeStamp << COUNT_BITS | count;
    }
}
