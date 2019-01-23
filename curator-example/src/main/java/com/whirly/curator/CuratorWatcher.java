package com.whirly.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-21 11:47
 **/
public class CuratorWatcher {
    private static final String zkServerIps = "master:2181,hadoop2:2181";

    public static void main(String[] args) throws Exception {
        final String nodePath = "/testZK";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10000, 5);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkServerIps)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy).build();
        try {
            client.start();
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath, "this is a test data".getBytes());

            final NodeCache cacheNode = new NodeCache(client, nodePath, false);
            cacheNode.start(true);  // true 表示启动时立即从Zookeeper上获取节点
            cacheNode.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    System.out.println("节点数据更新，新的内容是： " + new String(cacheNode.getCurrentData().getData()));
                }
            });
            for (int i = 0; i < 5; i++) {
                client.setData().forPath(nodePath, ("new test data " + i).getBytes());
                Thread.sleep(1000);
            }
            Thread.sleep(10000); // 等待100秒，手动在 zkCli 客户端操作节点，触发事件
        } finally {
            client.delete().deletingChildrenIfNeeded().forPath(nodePath);
            client.close();
            System.out.println("客户端关闭......");
        }
    }
}
