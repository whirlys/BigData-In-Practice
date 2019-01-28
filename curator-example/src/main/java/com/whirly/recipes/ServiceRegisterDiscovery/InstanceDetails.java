package com.whirly.recipes.ServiceRegisterDiscovery;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-23 16:38
 **/
@AllArgsConstructor
@Data
public class InstanceDetails {
    public static final String ROOT_PATH = "/testZK/service";

    /**
     * 该服务拥有哪些方法
     */
    public Map<String, Service> services = new HashMap<>();

    /**
     * 服务描述
     */
    private String serviceDesc;

    public InstanceDetails() {
        this.serviceDesc = "";
    }

    @Data
    public static class Service {
        /**
         * 方法名称
         */
        private String methodName;

        /**
         * 方法描述
         */
        private String desc;

        /**
         * 方法所需要的参数列表
         */
        private List<String> params;

    }
}
