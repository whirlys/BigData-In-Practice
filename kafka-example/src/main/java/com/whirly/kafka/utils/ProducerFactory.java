package com.whirly.kafka.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 15:53
 **/
public class ProducerFactory<K, V> {
    public static final String brokerList = "192.168.0.101:9092";
    public static final String topic = "topic.demo";

    public KafkaProducer<K, V> create() {
        return create(null);
    }

    /**
     * 工厂方法，创建kafka生产者客户端
     */
    public KafkaProducer<K, V> create(Properties properties) {
        Properties defaultProperties = new Properties();
        defaultProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        defaultProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        if (properties != null) {
            // 合并
            for (String key : properties.stringPropertyNames()) {
                defaultProperties.put(key, properties.getProperty(key));
            }
        }
        KafkaProducer<K, V> producer = new KafkaProducer<>(properties);
        return producer;
    }
}
