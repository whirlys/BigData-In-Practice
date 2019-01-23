package com.whirly.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-21 10:06
 **/
public class CuratorCrud {
    // 集群模式则是多个ip
    private static final String zkServerIps = "master:2181,hadoop2:2181";

    public static void main(String[] args) throws Exception {
        // 创建节点
        String nodePath = "/testZK"; // 节点路径
        byte[] data = "this is a test data".getBytes(); // 节点数据
        byte[] newData = "new test data".getBytes(); // 节点数据

        // 设置重连策略ExponentialBackoffRetry, baseSleepTimeMs：初始sleep的时间,maxRetries：最大重试次数,maxSleepMs：最大重试时间
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10000, 5);
        //（推荐）curator链接zookeeper的策略:RetryNTimes n：重试的次数 sleepMsBetweenRetries：每次重试间隔的时间
        // RetryPolicy retryPolicy = new RetryNTimes(3, 5000);
        // （不推荐） curator链接zookeeper的策略:RetryOneTime sleepMsBetweenRetry:每次重试间隔的时间,这个策略只会重试一次
        // RetryPolicy retryPolicy2 = new RetryOneTime(3000);
        // 永远重试，不推荐使用
        // RetryPolicy retryPolicy3 = new RetryForever(retryIntervalMs)
        // curator链接zookeeper的策略:RetryUntilElapsed maxElapsedTimeMs:最大重试时间 sleepMsBetweenRetries:每次重试间隔 重试时间超过maxElapsedTimeMs后，就不再重试
        // RetryPolicy retryPolicy4 = new RetryUntilElapsed(2000, 3000);

        // Curator客户端
        CuratorFramework client = null;
        // 实例化Curator客户端，Curator的编程风格可以让我们使用方法链的形式完成客户端的实例化
        client = CuratorFrameworkFactory.builder()  // 使用工厂类来建造客户端的实例对象
                .connectString(zkServerIps) // 放入zookeeper服务器ip
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)  // 设定会话时间以及重连策略
                // .namespace("testApp")    // 隔离的命名空间
                .build(); // 建立连接通道
        // 启动Curator客户端
        client.start();

        boolean isZkCuratorStarted = client.getState().equals(CuratorFrameworkState.STARTED);
        System.out.println("当前客户端的状态：" + (isZkCuratorStarted ? "连接中..." : "已关闭..."));
        try {
            // 检查节点是否存在
            Stat s = client.checkExists().forPath(nodePath);
            if (s == null) {
                System.out.println("节点不存在，创建节点");
                // 创建节点
                String result = client.create()
                        .creatingParentsIfNeeded()    // 创建父节点，也就是会递归创建
                        .withMode(CreateMode.PERSISTENT) // 节点类型，PERSISTENT是持久节点
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE) // 节点的ACL权限
                        .forPath(nodePath, data);
                System.out.println(result + "节点，创建成功...");
            } else {
                System.out.println("节点已存在，" + s);
            }
            getData(client, nodePath);  // 输出节点信息

            // 更新指定节点的数据
            int version = s == null ? 0 : s.getVersion();  // 版本不一致时的异常：KeeperErrorCode = BadVersion
            Stat resultStat = client.setData().withVersion(version)   // 指定数据版本
                    .forPath(nodePath, newData);    // 需要修改的节点路径以及新数据
            System.out.println("更新节点数据成功");
            getData(client, nodePath);  // 输出节点信息

            // 删除节点
            client.delete().guaranteed()    // 如果删除失败，那么在后端还是会继续删除，直到成功
                    .deletingChildrenIfNeeded() // 子节点也一并删除，也就是会递归删除
                    .withVersion(resultStat.getVersion())
                    .forPath(nodePath);
            System.out.println("删除节点：" + nodePath);
            Thread.sleep(1000);
        } finally {
            // 关闭客户端
            if (!client.getState().equals(CuratorFrameworkState.STOPPED)) {
                System.out.println("关闭客户端.....");
                client.close();
            }
            isZkCuratorStarted = client.getState().equals(CuratorFrameworkState.STARTED);
            System.out.println("当前客户端的状态：" + (isZkCuratorStarted ? "连接中..." : "已关闭..."));
        }
    }

    /**
     * 读取节点的数据
     */
    private static byte[] getData(CuratorFramework client, String nodePath) throws Exception {
        Stat stat = new Stat();
        byte[] nodeData = client.getData().storingStatIn(stat).forPath(nodePath);
        System.out.println("节点 " + nodePath + " 的数据为：" + new String(nodeData));
        System.out.println("该节点的数据版本号为：" + stat.getVersion() + "\n");
        return nodeData;
    }
}
