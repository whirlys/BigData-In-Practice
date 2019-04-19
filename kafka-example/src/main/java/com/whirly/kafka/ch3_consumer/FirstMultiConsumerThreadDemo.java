package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.utils.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @description: 第一种多线程方式，有几个分区就开启几个消费者线程
 * @author: 赖键锋
 * @create: 2019-04-18 16:36
 **/
public class FirstMultiConsumerThreadDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        String topic = "topic.demo";
        // 需要创建一个有4个分区的topic
        // bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --alter --partitions 4 --topic topic.demo
        int consumerThreadNum = 4;
        for (int i = 0; i < consumerThreadNum; i++) {
            Thread thread = new KafkaConsumerThread(props, topic);
            thread.setName("KafkaConsumerThread-" + i);
            thread.start();
        }
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic) {
            this.kafkaConsumer = new ConsumerFactory<String, String>().create();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        //process record.
                        System.out.println(getName() + " -> " + record.value());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}
