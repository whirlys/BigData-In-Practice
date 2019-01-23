package com.whirly.recipes.ServiceRegisterDiscovery;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-22 21:56
 **/
@AllArgsConstructor
@Data
public class ServiceDetail {
    //服务注册的根路径
    public static final String REGISTER_ROOT_PATH = "/testZK/discovery";

    private String desc;
    private int weight;
}
