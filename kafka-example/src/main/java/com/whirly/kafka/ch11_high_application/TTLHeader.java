package com.whirly.kafka.ch11_high_application;

import org.apache.kafka.common.header.Header;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:49
 **/
public class TTLHeader implements Header {
    private long ttl;//超时时间，单位为秒

    public TTLHeader(long ttl) {
        this.ttl = ttl;
    }

    @Override
    public String key() {
        return "ttl";
    }

    @Override
    public byte[] value() { //将long类型转成byte[]类型
        long res = this.ttl;
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((res >> offset) & 0xff);
        }
        return buffer;
    }
}
