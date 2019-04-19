package com.whirly.kafka.ch2_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @description: 测试使用自定义的序列化器
 * @author: 赖键锋
 * @create: 2019-04-18 12:09
 **/
public class Test {
    public static final String brokerList = "192.168.0.101:9092";
    public static final String topic = "topic.demo";

    public static void main(String[] args) {
        testCompanySerializer();
        testSelfPartition();
        testSelfProducerInterceptor();
    }

    public static void testCompanySerializer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 使用 字符串序列化自定义类，将报异常：
        // Exception in thread "main" org.apache.kafka.common.errors.SerializationException:
        // Can't convert value of class com.whirly.kafka.ch2_producer.Company to
        // class org.apache.kafka.common.serialization.StringSerializer specified in value.serializer
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 使用自定义的序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put("bootstrap.servers", brokerList);

        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("whirly").address("China").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void testSelfPartition() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 使用自定义的分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        properties.put("bootstrap.servers", brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "test DemoPartitioner");
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void testSelfProducerInterceptor() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 使用自定义的生产者拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, DemoProducerInterceptor.class.getName());
        properties.put("bootstrap.servers", brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "test DemoProducerInterceptor");
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
