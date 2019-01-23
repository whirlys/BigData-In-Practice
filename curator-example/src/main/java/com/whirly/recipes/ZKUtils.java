package com.whirly.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-22 12:24
 **/
public class ZKUtils {
    private static final String zkServerIps = "master:2181,hadoop2:2181";

    public static synchronized CuratorFramework getClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkServerIps)
                .sessionTimeoutMs(6000).connectionTimeoutMs(3000) //.namespace("LeaderLatchTest")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        return client;
    }
}
