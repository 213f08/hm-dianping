-- 秒杀资格判断：库存 + 一人一单，全部原子执行
-- 参数：ARGV[1]=voucherId, ARGV[2]=userId, ARGV[3]=orderId

-- 1. 接收参数
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- 2. 库存 key（String）、已下单用户 key（Set）
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 3. 判断库存，不足返回 1
if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end

-- 4. 判断是否重复下单，已下单返回 2
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 5. 扣库存
redis.call('incrby', stockKey, -1)
-- 6. 记录该用户已下单
redis.call('sadd', orderKey, userId)
-- 7. 发消息到 Stream（第 7 章消费）
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
-- 8. 返回 0 表示有购买资格
return 0
