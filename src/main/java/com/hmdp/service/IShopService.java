package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IShopService extends IService<Shop> {


    Result updateShop(Shop shop);

    /**
     * 根据 id 查询商铺（入口方法，负责包装 Result）
     */
    Result queryById(Long id);
}
