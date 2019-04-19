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

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 16:54
 **/
public class OffsetCommitSyncSingle {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new ConsumerFactory<String, String>().create();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                    long offset = record.offset();
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections
                            .singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                }
            }

//            TopicPartition tp1 = new TopicPartition(topic, 0);
//            TopicPartition tp2 = new TopicPartition(topic, 1);
//            TopicPartition tp3 = new TopicPartition(topic, 2);
//            TopicPartition tp4 = new TopicPartition(topic, 3);
//            System.out.println(consumer.committed(tp1) + " : " + consumer.position(tp1));
//            System.out.println(consumer.committed(tp2) + " : " + consumer.position(tp2));
//            System.out.println(consumer.committed(tp3) + " : " + consumer.position(tp3));
//            System.out.println(consumer.committed(tp4) + " : " + consumer.position(tp4));
        } finally {
            consumer.close();
        }
    }
}
