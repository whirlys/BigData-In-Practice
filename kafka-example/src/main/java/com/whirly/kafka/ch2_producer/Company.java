package com.whirly.kafka.ch2_producer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description: 测试序列化用的类
 * @author: 赖键锋
 * @create: 2019-04-18 11:59
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {
    private String name;
    private String address;
}
