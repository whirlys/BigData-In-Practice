package com.whirly.kafka.ch2_producer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @description: 自定义序列化
 * @author: 赖键锋
 * @create: 2019-04-18 12:01
 **/
public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    /**
     * 自定义序列化操作
     *
     * @param topic
     * @param company
     * @return
     */
    @Override
    public byte[] serialize(String topic, Company company) {
        if (null == company) {
            return null;
        }
        byte[] name, address;
        try {
            if (company.getName() != null) {
                name = company.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (company.getAddress() != null) {
                address = company.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }


    @Override
    public void close() {

    }
}
