package com.whirly.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description: Kafka 消费者工厂类
 * @author: 赖键锋
 * @create: 2019-04-18 16:09
 **/
public class ConsumerFactory<K, V> {
    public static final String brokerList = "192.168.0.101:9092";
    public static final String topic = "topic.demo";
    public static final String groupId = "group.demo";

    public KafkaConsumer<K, V> create() {
        return create(null);
    }

    /**
     * 工厂方法创建 kafka 消费者客户端
     */
    public KafkaConsumer<K, V> create(Properties properties) {
        Properties defaultProperties = new Properties();
        defaultProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        defaultProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        defaultProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        defaultProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if (properties != null) {
            // 合并
            for (String key : properties.stringPropertyNames()) {
                defaultProperties.put(key, properties.getProperty(key));
            }
        }
        KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(defaultProperties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}
