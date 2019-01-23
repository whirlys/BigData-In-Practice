package com.whirly.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-21 21:01
 **/
public class CuratorPCWatcher {
    private static final String zkServerIps = "master:2181,hadoop2:2181";

    public static void main(String[] args) throws Exception {
        final String nodePath = "/testZK";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(10000, 5);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkServerIps)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy).build();
        client.start();
        try {
            // 为子节点添加watcher，PathChildrenCache: 监听数据节点的增删改，可以设置触发的事件
            final PathChildrenCache childrenCache = new PathChildrenCache(client, nodePath, true);

            /**
             * StartMode: 初始化方式
             *  - POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
             *  - NORMAL：异步初始化
             *  - BUILD_INITIAL_CACHE：同步初始化
             */
            childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            // 列出子节点数据列表，需要使用BUILD_INITIAL_CACHE同步初始化模式才能获得，异步是获取不到的
            List<ChildData> childDataList = childrenCache.getCurrentData();
            System.out.println("当前节点的子节点详细数据列表：");
            for (ChildData childData : childDataList) {
                System.out.println("\t* 子节点路径：" + new String(childData.getPath()) + "，该节点的数据为：" + new String(childData.getData()));
            }

            // 添加事件监听器
            childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                    // 通过判断event type的方式来实现不同事件的触发
                    if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {  // 子节点初始化时触发
                        System.out.println("子节点初始化成功");
                    } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {  // 添加子节点时触发
                        System.out.print("子节点：" + event.getData().getPath() + " 添加成功，");
                        System.out.println("该子节点的数据为：" + new String(event.getData().getData()));
                    } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发
                        System.out.println("子节点：" + event.getData().getPath() + " 删除成功");
                    } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发
                        System.out.print("子节点：" + event.getData().getPath() + " 数据更新成功，");
                        System.out.println("子节点：" + event.getData().getPath() + " 新的数据为：" + new String(event.getData().getData()));
                    }
                }
            });
            Thread.sleep(100000); // sleep 100秒，在 zkCli.sh 操作子节点，注意查看控制台的输出
        } finally {
            client.close();
        }
    }
}
