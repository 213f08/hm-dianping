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

    /**
     * 秒杀下单入口：前置校验（券存在/时间/库存快照）
     */
    Result seckillVoucher(Long voucherId);

    /**
     * 真正下单：一人一单校验 + 扣库存 + 建订单
     *
     * 单独抽成接口方法，是为了能通过【代理对象】调用它，让 @Transactional 生效
     * （同类内部 this 调用不会走代理，事务注解会失效）。
     */
    Result createVoucherOrder(Long voucherId);
}
