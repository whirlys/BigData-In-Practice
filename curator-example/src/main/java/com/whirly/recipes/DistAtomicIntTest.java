package com.whirly.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.RetryNTimes;

import java.util.concurrent.CountDownLatch;

/**
 * @program: curator-example
 * @description: 分布式计数器，典型应用场景：统计在线人数
 * @author: 赖键锋
 * @create: 2019-01-22 12:23
 **/
public class DistAtomicIntTest {
    private static final String distAtomicPath = "/testZK/dist_atomic";
    private static final int clientNums = 30;
    private static CountDownLatch countDownLatch = new CountDownLatch(clientNums);

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < clientNums; i++) {
            String name = "client#" + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        CuratorFramework client = ZKUtils.getClient();
                        client.start();
                        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, distAtomicPath, new RetryNTimes(3, 1000));
                        for (int j = 0; j < 10; j++) {
                            AtomicValue<Integer> rc = atomicInteger.add(1);
                            System.out.println(name + " Result: " + rc.succeeded() + ", postValue: " + rc.postValue());
                            Thread.sleep(100);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            }).start();
        }
        countDownLatch.await();
    }
}
