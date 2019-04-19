package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.utils.ConsumerFactory;
import com.whirly.kafka.utils.ProducerFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @description: 消费者拦截器，实现一个简单 TTL 功能，使用消息的 time stamp 字段来判定是否过期
 * @author: 赖键锋
 * @create: 2019-04-18 15:44
 **/
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    /**
     * 在 poll 返回之前调研
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println("before:" + records);
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : tpRecords) {
                // 如果消息戳与当前的时间戳相差超过 10 秒则判定为过期，只返回不过期的数据
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) ->
                System.out.println(tp + ":" + offset.offset()));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());

        KafkaConsumer consumer = new ConsumerFactory<String, String>().create(properties);
        try {
            // 开启生产者， 发送一些消息，超过10秒钟之后再执行本方法
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : records) {
                    System.out.println(String.format("%s-%s-%s-%s",
                            record.topic(), record.partition(), record.offset(), record.value()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
