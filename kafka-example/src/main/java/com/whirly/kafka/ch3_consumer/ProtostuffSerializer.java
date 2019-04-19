package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.ch2_producer.Company;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @description: ProtocolBuffer 序列化
 * @author: 赖键锋
 * @create: 2019-04-18 13:34
 **/
public class ProtostuffSerializer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            protostuff = ProtobufIOUtil.toByteArray(data, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
        return protostuff;
    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {
        String brokerList = "192.168.0.101:9092";
        String topic = "topic.serialization";
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 自定义的 ProtostuffSerializer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtostuffSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);

        Company company = Company.builder().name("whirly").address("中国").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
        try {
            producer.send(record).get();
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
