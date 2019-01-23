package com.whirly.recipes.ServiceRegisterDiscovery;

import com.whirly.recipes.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.concurrent.TimeUnit;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-22 21:59
 **/
public class AppServer2 {
    public static void main(String[] args) throws Exception {
        CuratorFramework client = ZKUtils.getClient();
        client.start();
        client.blockUntilConnected();

        /**
         * 指定服务的 地址，端口，名称
         */
        ServiceInstanceBuilder<ServiceDetail> sib = ServiceInstance.builder();
        sib.address("192.168.1.100");
        sib.port(8866);
        sib.name("tomcat");
        sib.payload(new ServiceDetail("主站web程序", 2));

        ServiceInstance<ServiceDetail> instance = sib.build();

        ServiceDiscovery<ServiceDetail> serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceDetail.class)
                .client(client)
                .serializer(new JsonInstanceSerializer<ServiceDetail>(ServiceDetail.class))
                .basePath(ServiceDetail.REGISTER_ROOT_PATH)
                .build();
        //服务注册
        serviceDiscovery.registerService(instance);
        serviceDiscovery.start();

        TimeUnit.SECONDS.sleep(70);

        serviceDiscovery.close();
        client.close();
    }
}
