package com.whirly.hbase.monitor;

import com.whirly.hbase.monitor.entity.HbaseSummary;
import com.whirly.hbase.monitor.entity.RegionServerInfo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @program: HbaseExamples
 * @description: 从 hbase jmx 中获取监控信息，然后解析到自己写的 HbaseSummary、RegionServerInfo 类中
 * hbase的jmx在1.0后分开了master和regionserver的jmx监控，其中master的页面在master_HOSTNAME:60010/jmx页面中，
 * regionserver在REGIONSERVER_HOSTNAME:60030/jmx中，
 * 如果想更详细的信息使用http://REGIONSERVER_HOSTNAME:60030/jmx?description=true
 * @author: 赖键锋
 * @create: 2019-02-16 22:29
 **/
public class HBaseUtil {
    public static long mbLength = 1048576L;
    public static long gbLength = 1073741824L;
    public static final String jmxServerUrlFormat = "%s/jmx?qry=%s";
    public static final String hbaseRegionJmxUrlFormat = "http://%s:%s/jmx?qry=%s";
    public static final String hbaseJmxServerUrl = "http://master:60010"; // 旧的端口是 16010
    public static final String hbaseRegionServerJmxPort = "60030"; // 旧的端口是 16310
    public static final String masterServer = "Hadoop:service=HBase,name=Master,sub=Server";
    public static final String regionServer = "Hadoop:service=HBase,name=RegionServer,sub=Server";
    public static final String regionIpc = "Hadoop:service=HBase,name=RegionServer,sub=IPC";

    public static HbaseSummary getHbaseSummary(StatefulHttpClient client) throws IOException {
        HbaseSummary hbaseSummary = new HbaseSummary();
        String hmasterUrl = String.format(jmxServerUrlFormat, hbaseJmxServerUrl, masterServer);
        MonitorMetrics monitorMetrics = client.get(MonitorMetrics.class, hmasterUrl, null, null);

        hbaseSummary
                .setNumDeadRegionServers((int) monitorMetrics.getMetricsValue("numDeadRegionServers"));
        hbaseSummary.setNumRegionServers((int) monitorMetrics.getMetricsValue("numRegionServers"));
        String liveRegionServers = monitorMetrics.getMetricsValue("tag.liveRegionServers")
                .toString();
        hbaseSummary.setLiveRegionServers(regionServerInfoReader(client, liveRegionServers, true));
        String deadRegionServers = monitorMetrics.getMetricsValue("tag.deadRegionServers")
                .toString();
        hbaseSummary.setDeadRegionServers(regionServerInfoReader(client, deadRegionServers, false));
        hbaseSummary.setHmasterNode("master");
        return hbaseSummary;
    }

    public static List<RegionServerInfo> regionServerInfoReader(StatefulHttpClient client,
                                                                String regionServerStr, boolean getInfo) {
        List<RegionServerInfo> regionServerInfos = new ArrayList<>();
        Date nowDate = new Date();
        for (String info : regionServerStr.split(";")) {
            String hostName = info.split(",")[0];
            RegionServerInfo regionServerInfo = new RegionServerInfo();
            if (getInfo) {
                try {
                    String regionServerUrl = String.format(hbaseRegionJmxUrlFormat, hostName, hbaseRegionServerJmxPort, regionServer);
                    MonitorMetrics hadoopMetrics = client
                            .get(MonitorMetrics.class, regionServerUrl, null, null);
                    regionServerInfo.setHostName(hostName);
                    regionServerInfo.setStoreCount((int) hadoopMetrics.getMetricsValue("storeCount"));
                    regionServerInfo.setRegionCount((int) hadoopMetrics.getMetricsValue("regionCount"));
                    regionServerInfo.setStoreFileCount((int) hadoopMetrics.getMetricsValue("storeFileCount"));
                    regionServerInfo.setMemStoreSize(
                            (int) hadoopMetrics.getMetricsValue("memStoreSize") / mbLength);
                    regionServerInfo.setStoreFileSize(
                            doubleFormat(hadoopMetrics.getMetricsValue("storeFileSize"),
                                    gbLength));
                    regionServerInfo
                            .setStaticIndexSize((int) hadoopMetrics.getMetricsValue("staticIndexSize"));
                    regionServerInfo
                            .setTotalRequestCount((int) hadoopMetrics.getMetricsValue("totalRequestCount"));
                    regionServerInfo
                            .setReadRequestCount((int) hadoopMetrics.getMetricsValue("readRequestCount"));
                    regionServerInfo
                            .setWriteRequestCount((int) hadoopMetrics.getMetricsValue("writeRequestCount"));
                    regionServerInfo
                            .setCompactedCellsCount((int) hadoopMetrics.getMetricsValue("compactedCellsCount"));
                    regionServerInfo.setMajorCompactedCellsCount(
                            (int) hadoopMetrics.getMetricsValue("majorCompactedCellsCount"));
                    regionServerInfo.setFlushedCellsSize(
                            doubleFormat(hadoopMetrics.getMetricsValue("flushedCellsSize"),
                                    gbLength));
                    regionServerInfo
                            .setBlockedRequestCount((int) hadoopMetrics.getMetricsValue("blockedRequestCount"));
                    regionServerInfo
                            .setSplitRequestCount((int) hadoopMetrics.getMetricsValue("splitRequestCount"));
                    regionServerInfo
                            .setSplitSuccessCount((int) hadoopMetrics.getMetricsValue("splitSuccessCount"));
                    regionServerInfo.setSlowGetCount((int) hadoopMetrics.getMetricsValue("slowGetCount"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    String regionIpcUrl = String
                            .format(hbaseRegionJmxUrlFormat, hostName, hbaseRegionServerJmxPort, regionIpc);
                    MonitorMetrics hadoopMetrics = client
                            .get(MonitorMetrics.class, regionIpcUrl, null, null);
                    regionServerInfo
                            .setNumOpenConnections((int) hadoopMetrics.getMetricsValue("numOpenConnections"));
                    regionServerInfo
                            .setNumActiveHandler((int) hadoopMetrics.getMetricsValue("numActiveHandler"));
                    regionServerInfo.setSentBytes(
                            doubleFormat(hadoopMetrics.getMetricsValue("sentBytes"), gbLength));
                    regionServerInfo.setReceivedBytes(
                            doubleFormat(hadoopMetrics.getMetricsValue("receivedBytes"),
                                    gbLength));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                regionServerInfo = new RegionServerInfo();
                regionServerInfo.setHostName(hostName);
            }
            regionServerInfos.add(regionServerInfo);
        }
        return regionServerInfos;
    }


    public static DecimalFormat df = new DecimalFormat("#.##");

    public static double doubleFormat(Object num, long unit) {
        double result = Double.parseDouble(String.valueOf(num)) / unit;
        return Double.parseDouble(df.format(result));
    }

    public static double doubleFormat(Object num) {
        double result = Double.parseDouble(String.valueOf(num));
        return Double.parseDouble(df.format(result));
    }
}
