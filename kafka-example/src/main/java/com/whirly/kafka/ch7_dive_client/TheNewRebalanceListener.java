package com.whirly.kafka.ch7_dive_client;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;

/**
 * @description:
 * @author: 赖键锋
 * @create: 2019-04-19 15:28
 **/
public class TheNewRebalanceListener implements ConsumerRebalanceListener {
    Collection<TopicPartition> lastAssignment = Collections.emptyList();

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
//            commitOffsets(partition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
//        for(TopicPartition partition: difference(lastAssignment, assignment)){
////            cleanupState(partition);
////        }
////        for (TopicPartition partition : difference(assignment, lastAssignment)) {
////            initializeState(partition);
////        }
////        for (TopicPartition partition : assignment) {
////            initializeOffset(partition);
////        }
////        this.lastAssignment = assignment;
    }
}
