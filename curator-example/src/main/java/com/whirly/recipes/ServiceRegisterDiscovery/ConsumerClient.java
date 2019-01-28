package com.whirly.recipes.ServiceRegisterDiscovery;

import com.whirly.recipes.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-23 16:41
 **/
public class ConsumerClient {
    public static void main(String[] args) throws Exception {
        ServiceDiscovery<InstanceDetails> serviceDiscovery;
        try (CuratorFramework client = ZKUtils.getClient()) {
            client.start();
            client.blockUntilConnected();

            serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                    .client(client)
                    .basePath(InstanceDetails.ROOT_PATH)
                    .serializer(new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class))
                    .build();
        }
        serviceDiscovery.start();

        boolean flag = true;

        //死循环来不停的获取服务列表,查看是否有新服务发布
        while (flag) {

            //根据名称获取服务
            Collection<ServiceInstance<InstanceDetails>> services = serviceDiscovery.queryForInstances("OrderService");
            if (services.size() == 0) {
                System.out.println("当前没有发现服务");
                Thread.sleep(10 * 1000);
                continue;
            }

            for (ServiceInstance<InstanceDetails> service : services) {

                //获取请求的scheme 例如：http://127.0.0.1:8080
                String uriSpec = service.buildUriSpec();
                //获取服务的其他信息
                InstanceDetails payload = service.getPayload();

                //服务描述
                String serviceDesc = payload.getServiceDesc();
                //获取该服务下的所有接口
                Map<String, InstanceDetails.Service> allService = payload.getServices();
                Set<Map.Entry<String, InstanceDetails.Service>> entries = allService.entrySet();

                for (Map.Entry<String, InstanceDetails.Service> entry : entries) {
                    System.out.println(serviceDesc + uriSpec
                            + "/" + service.getName()
                            + "/" + entry.getKey()
                            + " 该方法需要的参数为："
                            + entry.getValue().getParams().toString());
                }
            }
            System.out.println("---------------------");
            Thread.sleep(10 * 1000);
        }

    }
}
