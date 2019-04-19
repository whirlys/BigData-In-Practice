package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.ch2_producer.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @description: 自定义proto反序列化器
 * @author: 赖键锋
 * @create: 2019-04-18 13:39
 **/
public class ProtostuffDeserializer implements Deserializer<Company> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company ans = new Company();
        ProtostuffIOUtil.mergeFrom(data, ans, schema);
        return ans;
    }

    public void close() {

    }

    public static void main(String[] args) {
        String brokerList = "192.168.0.101:9092";
        String topic = "topic.serialization";
        String groupId = "group.demo";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtostuffDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, Company> consumer = new KafkaConsumer<String, Company>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                System.out.println(String.format("%s-%s-%s-%s",
                        record.topic(), record.partition(), record.offset(), record.value()));
                // 成功反序列化，输出：topic.serialization-0-1-Company(name=whirly, address=中国)
            }
        }
    }
}
