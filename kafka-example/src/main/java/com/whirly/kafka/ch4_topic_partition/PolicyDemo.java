package com.whirly.kafka.ch4_topic_partition;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * @description: 合法性验证
 * @author: 赖键锋
 * @create: 2019-04-18 22:27
 **/
public class PolicyDemo implements CreateTopicPolicy {
    public void configure(Map<String, ?> configs) {
    }

    public void close() throws Exception {
    }

    public void validate(RequestMetadata requestMetadata)
            throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null ||
                requestMetadata.replicationFactor() != null) {
            if (requestMetadata.numPartitions() < 5) {
                throw new PolicyViolationException("Topic should have at " +
                        "least 5 partitions, received: " +
                        requestMetadata.numPartitions());
            }
            if (requestMetadata.replicationFactor() <= 1) {
                throw new PolicyViolationException("Topic should have at " +
                        "least 2 replication factor, recevied: " +
                        requestMetadata.replicationFactor());
            }
        }
    }
}