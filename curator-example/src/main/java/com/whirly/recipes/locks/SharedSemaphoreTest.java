package com.whirly.recipes.locks;

import com.whirly.recipes.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-27 16:01
 **/
public class SharedSemaphoreTest {
    private static final int MAX_LEASE = 10;
    private static final String PATH = "/testZK/semaphore";
    private static final FakeLimitedResource resource = new FakeLimitedResource();

    public static void main(String[] args) throws Exception {
        CuratorFramework client = ZKUtils.getClient();
        client.start();
        InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, PATH, MAX_LEASE);
        Collection<Lease> leases = semaphore.acquire(5);
        System.out.println("获取租约数量：" + leases.size());
        Lease lease = semaphore.acquire();
        System.out.println("获取单个租约");
        resource.use(); // 使用资源
        // 再次申请获取5个leases，此时leases数量只剩4个，不够，将超时
        Collection<Lease> leases2 = semaphore.acquire(5, 10, TimeUnit.SECONDS);
        System.out.println("获取租约，如果超时将为null： " + leases2);
        System.out.println("释放租约");
        semaphore.returnLease(lease);
        // 再次申请获取5个，这次刚好够
        leases2 = semaphore.acquire(5, 10, TimeUnit.SECONDS);
        System.out.println("获取租约，如果超时将为null： " + leases2);
        System.out.println("释放集合中的所有租约");
        semaphore.returnAll(leases);
        semaphore.returnAll(leases2);
        client.close();
        System.out.println("结束!");
    }
}
