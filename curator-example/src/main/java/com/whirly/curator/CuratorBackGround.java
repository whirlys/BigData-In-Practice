package com.whirly.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.*;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-21 11:15
 **/
public class CuratorBackGround {
    private static final String zkServerIps = "master:2181,hadoop2:2181";

    public static void main(String[] args) throws Exception {
        CountDownLatch samphore = new CountDownLatch(2);
        ExecutorService tp = Executors.newFixedThreadPool(2);   // 线程池
        String nodePath = "/testZK";
        byte[] data = "this is a test data".getBytes();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10000, 5);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkServerIps)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy).build();
        client.start();

        // 异步创建节点，传入 ExecutorService，这样比较复杂的就会放到线程池中执行
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("event[code: " + curatorEvent.getResultCode() + ", type: " + curatorEvent.getType() + "]");
                        System.out.println("当前线程：" + Thread.currentThread().getName());
                        samphore.countDown();
                    }
                }, tp).forPath(nodePath, data); // 此处传入 ExecutorService tp

        // 异步创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("event[code: " + curatorEvent.getResultCode() + ", type: " + curatorEvent.getType() + "]");
                        System.out.println("当前线程：" + Thread.currentThread().getName());
                        samphore.countDown();
                    }
                }).forPath(nodePath, data); // 此处没有传入 ExecutorService tp

        samphore.await();
        tp.shutdown();
    }
}
