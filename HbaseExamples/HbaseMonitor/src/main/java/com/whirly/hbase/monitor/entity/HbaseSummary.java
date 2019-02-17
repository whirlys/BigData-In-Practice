package com.whirly.hbase.monitor.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.List;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 14:13
 **/
@Getter
@Setter
public class HbaseSummary {
    //"name": "Hadoop:service=HBase,name=Master,sub=Server"
    private String hmasterNode;
    private String status;
    //处于可用状态的regionserver汇总
    private List<RegionServerInfo> liveRegionServers;
    //处于不可用状态的regionserver汇总
    private List<RegionServerInfo> deadRegionServers;
    //处于可用状态的regionserver数量
    private int numRegionServers;
    //处于不可用状态的regionserver数量
    private int numDeadRegionServers;
    private Date createTime;

    public void printInfo() {
        System.out.println("HBASE SUMMARY INFO");
        System.out.println(String.format("numRegionServers:%d\nnumDeadRegionServers%d\n", numRegionServers,
                numDeadRegionServers));
        liveRegionServers.forEach(regionServerInfo -> {
            System.out.println(String.format("hostName:%s\nregionCount:%s", regionServerInfo.getHostName(),
                    regionServerInfo.getRegionCount()));

        });
        System.out.println("----------------------");
    }
}
