package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    /** 秒杀下单入口：前置校验 + 加锁，再调用 createVoucherOrder */
    Result seckillVoucher(Long voucherId);

    /** 真正下单：一人一单 + 扣库存 + 建订单（需代理调用，@Transactional 才生效） */
    Result createVoucherOrder(Long voucherId);
}
