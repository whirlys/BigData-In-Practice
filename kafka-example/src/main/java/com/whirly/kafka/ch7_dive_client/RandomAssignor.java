package com.whirly.kafka.ch7_dive_client;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:27
 **/
public class RandomAssignor extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }

        //针对每一个主题进行分区分配
        for (Map.Entry<String, List<String>> topicEntry :
                consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue();
            int consumerSize = consumersForTopic.size();
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null) {
                continue;
            }

            //当前主题下的所有分区
            List<TopicPartition> partitions =
                    AbstractPartitionAssignor.partitions(topic,
                            numPartitionsForTopic);
            //将每个分区随机分配给一个消费者
            for (TopicPartition partition : partitions) {
                int rand = new Random().nextInt(consumerSize);
                String randomConsumer = consumersForTopic.get(rand);
                assignment.get(randomConsumer).add(partition);
            }
        }
        return assignment;
    }

    @Override
    public String name() {
        return "name";
    }

    private Map<String, List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(res, topic, consumerId);
            }
        }
        return res;
    }
}
