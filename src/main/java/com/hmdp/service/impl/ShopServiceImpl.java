package com.hmdp.service.impl;

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

    @Override
    public Result queryById(Long id) {
//        从redis查询商铺缓存
        String key=CACHE_SHOP_KEY+id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        } else if (shopJson != null) {  // 有 key 但是空值"的情况 → 说明之前查过且不存在 → 直接返回，不查库
            return Result.fail("店铺不存在");
        }
//        不存在，查询数据库
        Shop shop = getById(id);
//        数据库不存在，返回错误
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL , TimeUnit.MINUTES);
            return Result.fail("店铺不存在");
        }
//        写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shop);
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
