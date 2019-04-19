package com.whirly.kafka.ch4_topic_partition;

import com.whirly.kafka.utils.AdminClientFactory;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @description: kafka admin Java 客户端简单操作
 * @author: 赖键锋
 * @create: 2019-04-18 21:22
 **/
public class KafkaAdminConfigOperation {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        describeTopicConfig();
//        alterTopicConfig();
//        addTopicPartitions();
    }

    /**
     * 查看 Topic 信息
     */
    public static void describeTopicConfig() throws ExecutionException, InterruptedException {
        AdminClient client = new AdminClientFactory().create();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, AdminClientFactory.topic);
        // 查看 topic 信息
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Config config = result.all().get().get(resource);
        System.out.println(config);
        // Config(entries=[ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=2.2-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
        client.close();
    }

    /**
     * 修改 topic
     */
    public static void alterTopicConfig() throws ExecutionException, InterruptedException {
        AdminClient client = new AdminClientFactory().create();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, AdminClientFactory.topic);
        // 注意，设置log.cleanup.policy=compact启用压缩策略，发送数据将受影响，可能遇到发送失败的问题
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singleton(entry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource, config);
        AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();
        client.close();
    }

    public static void addTopicPartitions() throws ExecutionException, InterruptedException {
        AdminClient client = new AdminClientFactory().create();

        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(AdminClientFactory.topic, newPartitions);
        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        result.all().get();
        client.close();
    }
}
