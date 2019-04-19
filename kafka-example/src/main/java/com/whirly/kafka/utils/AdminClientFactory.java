package com.whirly.kafka.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 21:25
 **/
public class AdminClientFactory {
    public static String brokerList = "192.168.0.101:9092";
    public static String topic = "topic.demo";

    public AdminClient create() {
        return create(null);
    }

    public AdminClient create(Properties properties) {
        Properties defaultProperties = new Properties();
        defaultProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        defaultProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        if (properties != null) {
            // 合并
            for (String key : properties.stringPropertyNames()) {
                defaultProperties.put(key, properties.getProperty(key));
            }
        }
        AdminClient client = AdminClient.create(defaultProperties);
        return client;
    }
}
