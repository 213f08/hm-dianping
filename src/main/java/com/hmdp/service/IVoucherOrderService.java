package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/** 订单服务接口 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    /** 秒杀下单入口：Lua 判断资格，通过则入队并返回订单 id */
    Result seckillVoucher(Long voucherId);

    /** 真正下单：一人一单兜底 + 扣库存 + 建订单（后台线程通过代理调用） */
    void createVoucherOrder(VoucherOrder voucherOrder);
}
