package com.whirly.hbase.monitor;

import java.io.IOException;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 22:29
 **/
public class MonitorApp {
    public static void main(String[] args) throws IOException {
        StatefulHttpClient client = new StatefulHttpClient(null);
        HadoopUtil.getHdfsSummary(client).printInfo();
        HBaseUtil.getHbaseSummary(client).printInfo();

        /**
         * 输出：
         * HDFS SUMMARY INFO
         * totalBlocks:204
         * totalFiles:313
         * numLiveDataNodes:1
         * nodeName:master
         * numBlocks:202
         * ----------------------
         * java.lang.NullPointerException
         * 	at com.whirly.hbase.monitor.HBaseUtil.regionServerInfoReader(HBaseUtil.java:102)
         * 	at com.whirly.hbase.monitor.HBaseUtil.getHbaseSummary(HBaseUtil.java:42)
         * 	at com.whirly.hbase.monitor.MonitorApp.main(MonitorApp.java:15)
         * HBASE SUMMARY INFO
         * numRegionServers:1
         * numDeadRegionServers0
         *
         * hostName:master
         * regionCount:5
         * ----------------------
         */
    }
}
