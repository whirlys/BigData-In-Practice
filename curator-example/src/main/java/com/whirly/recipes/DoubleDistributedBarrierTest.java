package com.whirly.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @program: curator-example
 * @description: 双栅栏
 * @author: 赖键锋
 * @create: 2019-01-28 11:43
 **/
public class DoubleDistributedBarrierTest {
    private static final String barrierPath = "/testZK/barrier";
    private static final int clientNums = 5;

    public static void main(String[] args) throws InterruptedException {
        CuratorFramework client = ZKUtils.getClient();
        client.start();
        ExecutorService service = Executors.newFixedThreadPool(clientNums);
        for (int i = 0; i < (clientNums + 2); ++i) {
            final DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, barrierPath, clientNums);
            final int index = i;
            Callable<Void> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Thread.sleep((long) (3 * Math.random()));
                    System.out.println("Client #" + index + " 等待");
                    if (false == barrier.enter(5, TimeUnit.SECONDS)) {
                        System.out.println("Client #" + index + " 等待超时！");
                        return null;
                    }
                    System.out.println("Client #" + index + " 进入");
                    Thread.sleep((long) (3000 * Math.random()));
                    barrier.leave();
                    System.out.println("Client #" + index + " 结束");
                    return null;
                }
            };
            service.submit(task);
        }
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
        client.close();
        System.out.println("OK!");

    }
}
