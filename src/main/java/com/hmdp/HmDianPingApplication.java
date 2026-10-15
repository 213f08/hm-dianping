package com.hmdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * exposeProxy = true 的作用：
 *   默认情况下 Spring AOP 不会把「当前代理对象」暴露到 ThreadLocal 里，
 *   于是 VoucherOrderServiceImpl 中的 AopContext.currentProxy() 会抛
 *   IllegalStateException: Cannot find current proxy。
 *   开启后，「一人一单」才能通过代理对象调用 createVoucherOrder()，
 *   使 @Transactional 真正生效（而不是被同类内部调用绕过）。
 */
@EnableAspectJAutoProxy(exposeProxy = true)
@MapperScan("com.hmdp.mapper")
@SpringBootApplication
public class HmDianPingApplication {

    public static void main(String[] args) {
        SpringApplication.run(HmDianPingApplication.class, args);
    }

}
