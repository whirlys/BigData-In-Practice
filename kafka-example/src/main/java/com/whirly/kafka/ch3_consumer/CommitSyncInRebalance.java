package com.whirly.kafka.ch3_consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description: 再均衡监听器
 * @author: 赖键锋
 * @create: 2019-04-18 15:37
 **/
public class CommitSyncInRebalance {
    public static final String brokerList = "192.168.0.101:9092";
    public static final String topic = "topic.demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        // 再均衡监听器
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            /**
             * 在再均衡开始之前，消费者停止读取之后被调用
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 提交消费位移，避免不必要的重复消费现象发生
                consumer.commitSync(currentOffsets);
            }

            /**
             * 在重新分配分区之后和消费者开始读取之前调研
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //do nothing.
            }
        });

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            consumer.close();
        }

    }
}
