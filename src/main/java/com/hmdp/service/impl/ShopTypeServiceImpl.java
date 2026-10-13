package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    private final StringRedisTemplate stringRedisTemplate;

    public ShopTypeServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public Result queryTypeList() {
        // 1. 从 Redis 查（key 是固定的，不用拼 id）
        String key = "cache:shopType:list";
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 命中 → 反序列化返回
        if (StrUtil.isNotBlank(json)) {
            List<ShopType> typeList = JSONUtil.toList(json, ShopType.class);
            return Result.ok(typeList);
        }
        // 3. 未命中 → 查库（带排序）→ 写 Redis → 返回
        List<ShopType> typeList = this.query().orderByAsc("sort").list();
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList),
                CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(typeList);
    }
}
