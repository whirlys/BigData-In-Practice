package com.whirly.hbase.monitor.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 14:10
 **/
@Getter
@Setter
public class DataNodeInfo {
    // datanode的hostname
    private String nodeName;
    // datanode的ip地址
    private String nodeAddr;
    // datanode的上次链接数量
    private int lastContact;
    // datanode上hdfs的已用空间 GB
    private double usedSpace;
    // datanode的状态
    private String adminState;
    // datanode上非hdfs的空间大小 GB
    private double nonDfsUsedSpace;
    // datanode上的总空间大小
    private double capacity;
    // datanode的block
    private int numBlocks;
    private double remaining;
    private double blockPoolUsed;
    private double blockPoolUsedPerent;
}
