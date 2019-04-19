package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.utils.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @description: 批量同步消费位移
 * @author: 赖键锋
 * @create: 2019-04-18 16:48
 **/
public class OffsetCommitSyncBatch {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new ConsumerFactory<String, String>().create();
        final int minBatchSize = 200;
        List<ConsumerRecord> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
                System.out.println(record.offset() + " : " + record.value());
            }
            if (buffer.size() >= minBatchSize) {
                //do some logical processing with buffer.
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}
