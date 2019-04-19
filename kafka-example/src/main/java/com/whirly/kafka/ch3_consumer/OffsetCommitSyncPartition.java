package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.utils.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 16:51
 **/
public class OffsetCommitSyncPartition {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new ConsumerFactory<String, String>().create();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        //do some logical processing.
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition,
                            new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
