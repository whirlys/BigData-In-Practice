package com.whirly.hbase.monitor.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 14:14
 **/
@Getter
@Setter
public class RegionServerInfo {
    //regionserver的hostname
    private String hostName;
    //"name" : "Hadoop:service=HBase,name=RegionServer,sub=Server"
    //Store个数
    private int storeCount;
    //Regionserver管理region数量
    private int regionCount;
    //Storefile个数
    private int storeFileCount;
    //Memstore大小
    private double memStoreSize;
    //Storefile大小
    private double storeFileSize;
    //该regionserver所管理的表索引大小
    private double staticIndexSize;
    //总请求数
    private int totalRequestCount;
    //读请求数
    private int readRequestCount;
    //写请求数
    private int writeRequestCount;
    //合并cell个数
    private int compactedCellsCount;
    //大合并cell个数
    private int majorCompactedCellsCount;
    //flush到磁盘的大小
    private double flushedCellsSize;
    //因memstore大于阈值而引发flush的次数
    private int blockedRequestCount;
    //region分裂请求次数
    private int splitRequestCount;
    //region分裂成功次数
    private int splitSuccessCount;
    //请求完成时间超过1000ms的次数
    private int slowGetCount;
    //"name" : "Hadoop:service=HBase,name=RegionServer,sub=IPC"
    //该regionserver打开的连接数
    private int numOpenConnections;
    //rpc handler数
    private int numActiveHandler;
    //收到数据量 GB
    private double sentBytes;
    //发出数据量 GB
    private double receivedBytes;
}
