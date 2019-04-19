package com.whirly.kafka.ch7_dive_client;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:24
 **/
public class BroadcastAssignor extends AbstractPartitionAssignor {
    @Override
    public String name() {
        return "broadcast";
    }

    private Map<String, List<String>> consumersPerTopic(
            Map<String, Subscription> consumerMetadata) {
        //(具体实现请参考 RandomAssignor 中的 consumersPerTopic()方法)
        return null;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        //Java8
        subscriptions.keySet().forEach(memberId ->
                assignment.put(memberId, new ArrayList<>()));

        //针对每一个主题，为每一个订阅的消费者分配所有的分区
        consumersPerTopic.entrySet().forEach(topicEntry -> {
            String topic = topicEntry.getKey();
            List<String> members = topicEntry.getValue();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null || members.isEmpty())
                return;
            List<TopicPartition> partitions = AbstractPartitionAssignor
                    .partitions(topic, numPartitionsForTopic);
            if (!partitions.isEmpty()) {
                members.forEach(memberId ->
                        assignment.get(memberId).addAll(partitions));
            }
        });
        return assignment;
    }
}
