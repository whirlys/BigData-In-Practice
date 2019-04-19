package com.whirly.kafka.ch1_quick_start;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @description: kafka 消费者客户端 demo
 * @author: 赖键锋
 * @create: 2019-04-18 00:49
 **/
public class ConsumerFastStart {
    public static final String brokerList = "192.168.0.101:9092";
    public static final String topic = "topic.demo";
    public static final String groupId = "group.demo1";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                System.out.println(String.format("%s-%s-%s-%s",
                        record.topic(), record.partition(), record.offset(), record.value()));
            }
        }
    }
}
