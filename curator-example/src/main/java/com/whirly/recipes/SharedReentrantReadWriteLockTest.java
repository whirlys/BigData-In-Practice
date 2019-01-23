package com.whirly.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * @program: curator-example
 * @description: 分布式读写锁 - 可以看到通过获得read锁生成的订单中是有重复的，而获取的写锁中是没有重复数据的。符合读写锁的特点
 * @author: 赖键锋
 * @create: 2019-01-22 01:13
 **/
public class SharedReentrantReadWriteLockTest {

    private static final int SECOND = 1000;
    private static final String lock_path = "/testZK/leader_selector";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = ZKUtils.getClient();
        client.start();
        // todo 在此可添加ConnectionStateListener监听
        System.out.println("Server connected...");
        final InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, lock_path);
        final CountDownLatch down = new CountDownLatch(1);
        for (int i = 0; i < 30; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        down.await();
                        if (index % 2 == 0) {
                            lock.readLock().acquire();
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss|SSS");
                            String orderNo = sdf.format(new Date());
                            System.out.println("[READ]生成的订单号是:" + orderNo);
                        } else {
                            lock.writeLock().acquire();
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss|SSS");
                            String orderNo = sdf.format(new Date());
                            System.out.println("[WRITE]生成的订单号是:" + orderNo);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (index % 2 == 0) {
                                lock.readLock().release();
                            } else {
                                lock.writeLock().release();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
        // 保证所有线程内部逻辑执行时间一致
        down.countDown();
        Thread.sleep(10 * SECOND);
        if (client != null) {
            client.close();
        }
        System.out.println("Server closed...");
    }

}
