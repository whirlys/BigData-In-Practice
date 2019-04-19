package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.utils.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 16:56
 **/
public class SeekDemo {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new ConsumerFactory<String, String>().create();
        consumer.poll(Duration.ofMillis(2000));
        Set<TopicPartition> assignment = consumer.assignment();
        System.out.println(assignment);
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 10);
        }
        // consumer.seek(new TopicPartition(ConsumerFactory.topic,0),10);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value());
            }
        }
    }
}
