package com.whirly.hbase.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 22:28
 **/
public class MonitorMetrics {

    List<Map<String, Object>> beans = new ArrayList<>();

    public List<Map<String, Object>> getBeans() {
        return beans;
    }

    public void setBeans(List<Map<String, Object>> beans) {
        this.beans = beans;
    }

    public Object getMetricsValue(String name) {
        if (beans.isEmpty()) {
            return null;
        }
        return beans.get(0).getOrDefault(name, null);
    }
}
