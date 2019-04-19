package com.whirly.kafka.ch2_producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @description: 自定义生产者拦截器
 * @author: 赖键锋
 * @create: 2019-04-18 02:28
 **/
public class DemoProducerInterceptor implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 在将消息序列化和计算分区之前会调用
     *
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 拦截器，仅在 value 之前添加前缀
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
    }

    /**
     * 消息被应答（ Acknowledgement ）之前或消息发送失败时调用,优先于用户设定的 Callback 之前执行
     *
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率=" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
