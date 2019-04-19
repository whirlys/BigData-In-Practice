package com.whirly.kafka.ch3_consumer;

import com.whirly.kafka.ch2_producer.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @description: 自定义反序列化器
 * @author: 赖键锋
 * @create: 2019-04-18 12:27
 **/
public class CompanyDeserailizer implements Deserializer<Company> {
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received " +
                    "by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }

        return Company.builder().name(name).address(address).build();
    }

    public void close() {
    }
}