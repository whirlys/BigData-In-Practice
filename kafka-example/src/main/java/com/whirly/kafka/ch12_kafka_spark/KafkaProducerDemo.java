package com.whirly.kafka.ch12_kafka_spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 21:25
 **/
public class KafkaProducerDemo {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-spark";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-demo-client");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        Random random = new Random();
        while (true) {
            int value = random.nextInt(10);
            ProducerRecord<String, String> message =
                    new ProducerRecord<>(topic, value + "");
            producer.send(message, (recordMetadata, e) -> {
                if (recordMetadata != null) {
                    System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition() + ":" +
                            recordMetadata.offset());
                }
            });
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
