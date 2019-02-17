package com.whirly.hbase.monitor;

import com.whirly.hbase.monitor.entity.DataNodeInfo;
import com.whirly.hbase.monitor.entity.HdfsSummary;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: HbaseExamples
 * @description: 从 HDFS jmx 获取监控信息，然后解析封装到自己写的 HdfsSummary、DataNodeInfo 类中
 * @author: 赖键锋
 * @create: 2019-02-16 14:10
 **/
public class HadoopUtil {
    public static long mbLength = 1048576L;
    public static long gbLength = 1073741824L;
    public static final String hadoopJmxServerUrl = "http://master:50070";
    public static final String jmxServerUrlFormat = "%s/jmx?qry=%s";
    public static final String nameNodeInfo = "Hadoop:service=NameNode,name=NameNodeInfo";
    public static final String fsNameSystemState = "Hadoop:service=NameNode,"
            + "name=FSNamesystemState";

    public static HdfsSummary getHdfsSummary(StatefulHttpClient client) throws IOException {
        HdfsSummary hdfsSummary = new HdfsSummary();
        String namenodeUrl = String.format(jmxServerUrlFormat, hadoopJmxServerUrl, nameNodeInfo);
        MonitorMetrics monitorMetrics = client.get(MonitorMetrics.class, namenodeUrl, null, null);
        hdfsSummary.setTotal(doubleFormat(monitorMetrics.getMetricsValue("Total"), gbLength));
        hdfsSummary.setDfsFree(doubleFormat(monitorMetrics.getMetricsValue("Free"), gbLength));
        hdfsSummary
                .setDfsUsed(doubleFormat(monitorMetrics.getMetricsValue("Used"), gbLength));
        hdfsSummary.setPercentUsed(
                doubleFormat(monitorMetrics.getMetricsValue("PercentUsed")));
        hdfsSummary.setSafeMode(monitorMetrics.getMetricsValue("Safemode").toString());
        hdfsSummary.setNonDfsUsed(
                doubleFormat(monitorMetrics.getMetricsValue("NonDfsUsedSpace"), gbLength));
        hdfsSummary.setBlockPoolUsedSpace(
                doubleFormat(monitorMetrics.getMetricsValue("BlockPoolUsedSpace"),
                        gbLength));
        hdfsSummary
                .setPercentBlockPoolUsed(
                        doubleFormat(monitorMetrics.getMetricsValue("PercentBlockPoolUsed")));
        hdfsSummary.setPercentRemaining(
                doubleFormat(monitorMetrics.getMetricsValue("PercentRemaining")));
        hdfsSummary.setTotalBlocks((int) monitorMetrics.getMetricsValue("TotalBlocks"));
        hdfsSummary.setTotalFiles((int) monitorMetrics.getMetricsValue("TotalFiles"));
        hdfsSummary.setMissingBlocks((int) monitorMetrics.getMetricsValue("NumberOfMissingBlocks"));
        String liveNodesJson = monitorMetrics.getMetricsValue("LiveNodes").toString();
        String deadNodesJson = monitorMetrics.getMetricsValue("DeadNodes").toString();
        List<DataNodeInfo> liveNodes = dataNodeInfoReader(liveNodesJson);
        List<DataNodeInfo> deadNodes = dataNodeInfoReader(deadNodesJson);
        hdfsSummary.setLiveDataNodeInfos(liveNodes);
        hdfsSummary.setDeadDataNodeInfos(deadNodes);

        String fsNameSystemStateUrl = String
                .format(jmxServerUrlFormat, hadoopJmxServerUrl,
                        fsNameSystemState);
        MonitorMetrics hadoopMetrics = client
                .get(MonitorMetrics.class, fsNameSystemStateUrl, null, null);
        hdfsSummary.setNumLiveDataNodes((int) hadoopMetrics.getMetricsValue("NumLiveDataNodes"));
        hdfsSummary.setNumDeadDataNodes((int) hadoopMetrics.getMetricsValue("NumDeadDataNodes"));
        hdfsSummary
                .setVolumeFailuresTotal((int) hadoopMetrics.getMetricsValue("VolumeFailuresTotal"));
        return hdfsSummary;
    }

    public static List<DataNodeInfo> dataNodeInfoReader(String jsonData) throws IOException {
        List<DataNodeInfo> dataNodeInfos = new ArrayList<DataNodeInfo>();
        Map<String, Object> nodes = JsonUtil.fromJsonMap(String.class, Object.class, jsonData);
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> info = (HashMap<String, Object>) node.getValue();
            String nodeName = node.getKey().split(":")[0];
            DataNodeInfo dataNodeInfo = new DataNodeInfo();
            dataNodeInfo.setNodeName(nodeName);
            dataNodeInfo.setNodeAddr(info.get("infoAddr").toString().split(":")[0]);
            dataNodeInfo.setLastContact((int) info.get("lastContact"));
            dataNodeInfo.setUsedSpace(doubleFormat(info.get("usedSpace"), gbLength));
            dataNodeInfo.setAdminState(info.get("adminState").toString());
            dataNodeInfo
                    .setNonDfsUsedSpace(doubleFormat(info.get("nonDfsUsedSpace"), gbLength));
            dataNodeInfo.setCapacity(doubleFormat(info.get("capacity"), gbLength));
            dataNodeInfo.setNumBlocks((int) info.get("numBlocks"));
            dataNodeInfo.setRemaining(doubleFormat(info.get("remaining"), gbLength));
            dataNodeInfo
                    .setBlockPoolUsed(doubleFormat(info.get("blockPoolUsed"), gbLength));
            dataNodeInfo.setBlockPoolUsedPerent(doubleFormat(info.get("blockPoolUsedPercent")));
            dataNodeInfos.add(dataNodeInfo);
        }
        return dataNodeInfos;
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
