package com.whirly.recipes.locks;

import com.whirly.recipes.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-27 16:21
 **/
public class MultiSharedLockTest {
    private static final String lockPath1 = "/testZK/MSLock1";
    private static final String lockPath2 = "/testZK/MSLock2";
    private static final FakeLimitedResource resource = new FakeLimitedResource();

    public static void main(String[] args) throws Exception {
        CuratorFramework client = ZKUtils.getClient();
        client.start();

        InterProcessLock lock1 = new InterProcessMutex(client, lockPath1); // 可重入锁
        InterProcessLock lock2 = new InterProcessSemaphoreMutex(client, lockPath2); // 不可重入锁
        // 组锁，多锁
        InterProcessMultiLock lock = new InterProcessMultiLock(Arrays.asList(lock1, lock2));
        if (!lock.acquire(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("不能获取多锁");
        }
        System.out.println("已获取多锁");
        System.out.println("是否有第一个锁: " + lock1.isAcquiredInThisProcess());
        System.out.println("是否有第二个锁: " + lock2.isAcquiredInThisProcess());
        try {
            resource.use(); // 资源操作
        } finally {
            System.out.println("释放多个锁");
            lock.release(); // 释放多锁
        }
        System.out.println("是否有第一个锁: " + lock1.isAcquiredInThisProcess());
        System.out.println("是否有第二个锁: " + lock2.isAcquiredInThisProcess());
        client.close();
        System.out.println("结束!");
    }
}
