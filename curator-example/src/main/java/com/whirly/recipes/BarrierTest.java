package com.whirly.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

/**
 * @program: curator-example
 * @description: 分布式屏障
 * @author: 赖键锋
 * @create: 2019-01-22 12:41
 **/
public class BarrierTest {
    private static final String barrierPath = "/testZK/barrier_path";
    private static final Integer clientNUms = 5;
    private static DistributedBarrier barrier;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < clientNUms; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        CuratorFramework client = ZKUtils.getClient();
                        client.start();
                        barrier = new DistributedBarrier(client, barrierPath);
                        System.out.println(Thread.currentThread().getName() + " 号 barrier 设置");
                        barrier.setBarrier();
                        barrier.waitOnBarrier();
                        System.out.println("启动...");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        Thread.sleep(2000);
        barrier.removeBarrier();
    }
}
