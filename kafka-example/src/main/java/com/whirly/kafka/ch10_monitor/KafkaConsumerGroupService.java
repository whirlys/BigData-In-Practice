package com.whirly.kafka.ch10_monitor;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:44
 **/
@Slf4j
public class KafkaConsumerGroupService {
    private String brokerList;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerGroupService(String brokerList) {
        this.brokerList = brokerList;
    }

    public void init() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        adminClient = AdminClient.create(props);
        kafkaConsumer = ConsumerGroupUtils.createNewConsumer(brokerList,
                "kafkaAdminClientDemoGroupId");
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    public List<PartitionAssignmentState> collectGroupAssignment(
            String group) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult groupResult = adminClient
                .describeConsumerGroups(Collections.singleton(group));
        ConsumerGroupDescription description =
                groupResult.all().get().get(group);

        List<TopicPartition> assignedTps = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        Collection<MemberDescription> members = description.members();
        if (members != null) {
            ListConsumerGroupOffsetsResult offsetResult = adminClient
                    .listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetResult
                    .partitionsToOffsetAndMetadata().get();
            if (offsets != null && !offsets.isEmpty()) {
                String state = description.state().toString();
                if (state.equals("Stable")) {
                    rowsWithConsumer = getRowsWithConsumer(description, offsets,
                            members, assignedTps, group);
                }
            }
            List<PartitionAssignmentState> rowsWithoutConsumer =
                    getRowsWithoutConsumer(description, offsets,
                            assignedTps, group);
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }

    private List<PartitionAssignmentState> getRowsWithConsumer(
            ConsumerGroupDescription description,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            Collection<MemberDescription> members,
            List<TopicPartition> assignedTps, String group) {
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        for (MemberDescription member : members) {
            MemberAssignment assignment = member.assignment();
            if (assignment == null) {
                continue;
            }
            Set<TopicPartition> tpSet = assignment.topicPartitions();
            if (tpSet.isEmpty()) {
                rowsWithConsumer.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .consumerId(member.consumerId()).host(member.host())
                        .clientId(member.clientId()).build());

            } else {
                Map<TopicPartition, Long> logSizes =
                        kafkaConsumer.endOffsets(tpSet);
                assignedTps.addAll(tpSet);
                List<PartitionAssignmentState> tempList = tpSet.stream()
                        .sorted(comparing(TopicPartition::partition))
                        .map(tp -> getPasWithConsumer(logSizes, offsets, tp,
                                group, member, description)).collect(toList());
                rowsWithConsumer.addAll(tempList);
            }
        }
        return rowsWithConsumer;
    }

    private PartitionAssignmentState getPasWithConsumer(
            Map<TopicPartition, Long> logSizes,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            TopicPartition tp, String group,
            MemberDescription member,
            ConsumerGroupDescription description) {
        long logSize = logSizes.get(tp);
        if (offsets.containsKey(tp)) {
            long offset = offsets.get(tp).offset();
            long lag = getLag(offset, logSize);
            return PartitionAssignmentState.builder().group(group)
                    .coordinator(description.coordinator()).lag(lag)
                    .topic(tp.topic()).partition(tp.partition())
                    .offset(offset).consumerId(member.consumerId())
                    .host(member.host()).clientId(member.clientId())
                    .logSize(logSize).build();
        } else {
            return PartitionAssignmentState.builder()
                    .group(group).coordinator(description.coordinator())
                    .topic(tp.topic()).partition(tp.partition())
                    .consumerId(member.consumerId()).host(member.host())
                    .clientId(member.clientId()).logSize(logSize).build();
        }
    }

    private static long getLag(long offset, long logSize) {
        long lag = logSize - offset;
        return lag < 0 ? 0 : lag;
    }

    private List<PartitionAssignmentState> getRowsWithoutConsumer(
            ConsumerGroupDescription description,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            List<TopicPartition> assignedTps, String group) {
        Set<TopicPartition> tpSet = offsets.keySet();

        return tpSet.stream()
                .filter(tp -> !assignedTps.contains(tp))
                .map(tp -> {
                    long logSize = 0;
                    Long endOffset = kafkaConsumer.
                            endOffsets(Collections.singleton(tp)).get(tp);
                    if (endOffset != null) {
                        logSize = endOffset;
                    }
                    long offset = offsets.get(tp).offset();
                    return PartitionAssignmentState.builder().group(group)
                            .coordinator(description.coordinator())
                            .topic(tp.topic()).partition(tp.partition())
                            .logSize(logSize).lag(getLag(offset, logSize))
                            .offset(offset).build();
                }).sorted(comparing(PartitionAssignmentState::getPartition))
                .collect(toList());
    }

    public static void main(String[] args) throws ExecutionException,
            InterruptedException {
        KafkaConsumerGroupService service =
                new KafkaConsumerGroupService("192.168.0.101:9092");
        service.init();
        List<PartitionAssignmentState> list =
                service.collectGroupAssignment("groupIdMonitor");
        ConsumerGroupUtils.printPasList(list);
    }
}

class ConsumerGroupUtils {
    public static KafkaConsumer<String, String> createNewConsumer(
            String brokerUrl, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public static void printPasList(List<PartitionAssignmentState> list) {
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s" +
                        " %-50s%-30s %s", "TOPIC", "PARTITION",
                "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG",
                "CONSUMER-ID", "HOST", "CLIENT-ID"));

        list.forEach(item ->
                System.out.println(String.format("%-40s %-10s %-15s " +
                                "%-15s %-10s %-50s%-30s %s",
                        item.getTopic(), item.getPartition(), item.getOffset(),
                        item.getLogSize(), item.getLag(),
                        Optional.ofNullable(item.getConsumerId()).orElse("-"),
                        Optional.ofNullable(item.getHost()).orElse("-"),
                        Optional.ofNullable(item.getClientId()).orElse("-"))));
    }
}

@Data
@Builder
class PartitionAssignmentState {
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
}

