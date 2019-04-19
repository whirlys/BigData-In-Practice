package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.utils.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-18 17:02
 **/
public class SeekToEnd {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new ConsumerFactory<String, String>().create();
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
//            consumer.seek(tp, offsets.get(tp));
            consumer.seek(tp, offsets.get(tp) + 1);
        }
        System.out.println(assignment);
        System.out.println(offsets);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value());
            }
        }

    }
}
