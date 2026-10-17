-- seckill qualification check: stock + one-order-per-user, all atomic
-- args: ARGV[1]=voucherId, ARGV[2]=userId, ARGV[3]=orderId

-- 1. receive args
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- 2. build keys: stock (String), ordered users (Set)
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 3. check stock, return 1 if insufficient
if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end

-- 4. check duplicate order, return 2 if already ordered
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 5. decrement stock
redis.call('incrby', stockKey, -1)

-- 6. record this user as ordered
redis.call('sadd', orderKey, userId)

-- 7. send message to stream (consumed in chapter 7)
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

-- 8. return 0 means qualified
return 0
