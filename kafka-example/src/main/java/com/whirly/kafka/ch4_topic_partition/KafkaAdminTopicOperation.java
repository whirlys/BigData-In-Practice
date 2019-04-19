package com.whirly.kafka.ch4_topic_partition;

import com.whirly.kafka.utils.AdminClientFactory;
import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @description: topic 管理操作
 * @author: 赖键锋
 * @create: 2019-04-18 22:15
 **/
public class KafkaAdminTopicOperation {
    public static String topic = "topic.admin.test";

    public static void describeTopic() {
        AdminClient client = new AdminClientFactory().create();

        DescribeTopicsResult result = client.describeTopics(Collections.singleton(topic));
        try {
            Map<String, TopicDescription> descriptionMap = result.all().get();
            System.out.println(descriptionMap.get(topic));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void createTopic() {
        AdminClient client = new AdminClientFactory().create();

//        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);

//        Map<String, String> configs = new HashMap<>();
//        configs.put("cleanup.policy", "compact");
//        newTopic.configs(configs);

        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        replicasAssignments.put(2, Arrays.asList(0));
        replicasAssignments.put(3, Arrays.asList(0));

        //创建一个主题：topic-demo，其中分区数为4，副本数为1
//        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
        NewTopic newTopic = new NewTopic(topic, replicasAssignments);

        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            // 查看主题创建结果： bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic topic.admin.test
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void deleteTopic() {
        AdminClient client = new AdminClientFactory().create();
        try {
            client.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        createTopic();
//        describeTopic();
//        deleteTopic();
    }

}
