package com.whirly.kafka.ch10_monitor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import kafka.admin.ConsumerGroupCommand;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:42
 **/
public class KafkaConsumerGroupAnother {

    public static void main(String[] args) throws IOException {
        String[] agrs = {"--describe", "--bootstrap-server",
                "192.168.0.101:9092", "--group", "groupIdMonitor"};
        ConsumerGroupCommand.ConsumerGroupCommandOptions options =
                new ConsumerGroupCommand.ConsumerGroupCommandOptions(agrs);
        ConsumerGroupCommand.ConsumerGroupService kafkaConsumerGroupService =
                new ConsumerGroupCommand.ConsumerGroupService(options);

        ObjectMapper mapper = new ObjectMapper();
        //1. 使用jackson-module-scala_2.11
        mapper.registerModule(new DefaultScalaModule());
        //2. 反序列化时忽略对象不存在的属性
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        //3. 将Scala对象序列化成JSON字符串
        //这里原本会有权限问题，通过序列化绕过
        String source = mapper.writeValueAsString(kafkaConsumerGroupService.
                collectGroupOffsets()._2.get());
        //4. 将JSON字符串反序列化成Java对象
        List<PartitionAssignmentStateAnother> target = mapper.readValue(source,
                getCollectionType(mapper,List.class,
                        PartitionAssignmentStateAnother.class));
        //5. 排序
        target.sort((o1, o2) -> o1.getPartition() - o2.getPartition());
        //6. 打印
        printPasList(target);
    }

    public static JavaType getCollectionType(ObjectMapper mapper,
                                             Class<?> collectionClass,
                                             Class<?>... elementClasses) {
        return mapper.getTypeFactory()
                .constructParametricType(collectionClass, elementClasses);
    }

    public static void printPasList(List<PartitionAssignmentStateAnother> list) {
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"));
        list.forEach(item -> System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                item.getTopic(), item.getPartition(), item.getOffset(), item.getLogSize(), item.getLag(),
                Optional.ofNullable(item.getConsumerId()).orElse("-"),
                Optional.ofNullable(item.getHost()).orElse("-"),
                Optional.ofNullable(item.getClientId()).orElse("-"))));
    }
}

@Data
@Builder
class PartitionAssignmentStateAnother {
    private String group;
    private Node coordinator;
    private String topic;
    private int partition;
    private long offset;
    private long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private long logSize;

    @Data
    public static class Node{
        public int id;
        public String idString;
        public String host;
        public int port;
        public String rack;
    }
}