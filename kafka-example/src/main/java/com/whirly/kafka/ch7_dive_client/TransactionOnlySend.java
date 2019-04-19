package com.whirly.kafka.ch7_dive_client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:32
 **/
public class TransactionOnlySend {
    public static final String topic = "topic-transaction";
    public static final String brokerList = "192.168.0.101:9092";
    public static final String transactionId = "transactionId";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,                StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            //处理业务逻辑并创建ProducerRecord
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "msg1");
            producer.send(record1);
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "msg2");
            producer.send(record2);
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, "msg3");
            producer.send(record3);
            //处理一些其它逻辑
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            producer.abortTransaction();
        }
        producer.close();
    }

}