package com.whirly.recipes.ServiceRegisterDiscovery;

import com.whirly.recipes.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;

import java.util.Collection;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-22 22:00
 **/
public class AppClient {
    public static void main(String[] args) throws Exception {
        try (CuratorFramework client = ZKUtils.getClient()) {
            client.start();
            client.blockUntilConnected();

            ServiceDiscovery<ServiceDetail> serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceDetail.class)
                    .client(client)
                    .basePath(ServiceDetail.REGISTER_ROOT_PATH)
                    .build();
            serviceDiscovery.start();

            //根据名称获取服务
            Collection<ServiceInstance<ServiceDetail>> services = serviceDiscovery.queryForInstances("tomcat");
            for (ServiceInstance<ServiceDetail> service : services) {
                System.out.println(service.getPayload());
                System.out.println(service.getAddress() + "\t" + service.getPort());
                System.out.println("---------------------");
            }

            serviceDiscovery.close();
            client.close();
        }
    }
}
