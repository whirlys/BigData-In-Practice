package com.whirly.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-22 01:16
 **/
public class ZKPathsTest {
    private static final String zkServerIps = "master:2181,hadoop2:2181";
    private static String path = "/testZK/zk_path";

    public static void main(String[] args) {
        CuratorFramework client = getClient();

    }

    private static synchronized CuratorFramework getClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkServerIps)
                .sessionTimeoutMs(6000).connectionTimeoutMs(3000) //.namespace("LeaderLatchTest")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        client.start();
        return client;
    }
}
