package com.whirly.kafka.ch3_consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 11:57
 **/
@Slf4j
public class KafkaConsumerAnalysis {
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
        // 配置一个客户端ID，不配的话kafka会自动生成一个，内容形式如“ consumer-1 ”
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.demo1");
        return props;
    }


    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
//        consumer.subscribe(Arrays.asList(topic));
        // 还可以使用正则表达式
        // consumer.subscribe(Pattern.compile("topic.test.*"));

        // 直接订阅分区
        List<TopicPartition> partitions = new ArrayList<>();
        // 查询指定主题的元数据信息
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        if (infos != null) {
            for (PartitionInfo info : infos) {
                // 订阅所有分区，也可以订阅一部分分区
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
            consumer.assign(partitions);
        }
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, offset = %s",
                            record.topic(), record.partition(), record.offset()));
                    System.out.println(String.format("key = %s, value = %s", record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            log.error("发生异常 ", e);
            // 取消订阅
            consumer.unsubscribe();
        } finally {
            consumer.close();
        }
    }
}
