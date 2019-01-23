package com.whirly.recipes.configcenter;

import com.alibaba.fastjson.JSON;
import com.whirly.recipes.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.sql.*;
import java.util.concurrent.CountDownLatch;

/**
 * @program: curator-example
 * @description: 配置中心 - 数据库切换
 * @author: 赖键锋
 * @create: 2019-01-22 17:11
 **/
public class ConfigCenterTest {
    // test 数据库
    private static final MysqlConfig mysqlConfig_1 = new MysqlConfig("jdbc:mysql://master:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false", "com.mysql.jdbc.Driver", "root", "123456");
    // test2 数据库
    private static final MysqlConfig mysqlConfig_2 = new MysqlConfig("jdbc:mysql://master:3306/test2?useUnicode=true&characterEncoding=utf-8&useSSL=false", "com.mysql.jdbc.Driver", "root", "123456");
    // 存储MySQL配置信息的节点路径
    private static final String configPath = "/testZK/jdbc/mysql";
    private static final Integer clientNums = 3;
    private static CountDownLatch countDownLatch = new CountDownLatch(clientNums);

    public static void main(String[] args) throws Exception {
        // 最开始时设置MySQL配置信息为 mysqlConfig_1
        setMysqlConfig(mysqlConfig_1);
        // 启动 clientNums 个线程，模拟分布式系统中的节点，
        // 从Zookeeper中获取MySQL的配置信息，查询数据
        for (int i = 0; i < clientNums; i++) {
            String clientName = "client#" + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    CuratorFramework client = ZKUtils.getClient();
                    client.start();
                    try {
                        Stat stat = new Stat();
                        // 如果要监听多个子节点则应该使用 PathChildrenCache
                        final NodeCache cacheNode = new NodeCache(client, configPath, false);
                        cacheNode.start(true);  // true 表示启动时立即从Zookeeper上获取节点

                        byte[] nodeData = cacheNode.getCurrentData().getData();
                        MysqlConfig mysqlConfig = JSON.parseObject(new String(nodeData), MysqlConfig.class);
                        queryMysql(clientName, mysqlConfig);    // 查询数据

                        cacheNode.getListenable().addListener(new NodeCacheListener() {
                            @Override
                            public void nodeChanged() throws Exception {
                                byte[] newData = cacheNode.getCurrentData().getData();
                                MysqlConfig newMysqlConfig = JSON.parseObject(new String(newData), MysqlConfig.class);
                                queryMysql(clientName, newMysqlConfig);    // 查询数据
                            }
                        });
                        Thread.sleep(20 * 1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        client.close();
                        countDownLatch.countDown();
                    }
                }
            }).start();
        }
        Thread.sleep(10 * 1000);
        System.out.println("\n---------10秒钟后将MySQL配置信息修改为 mysqlConfig_2---------\n");
        setMysqlConfig(mysqlConfig_2);
        countDownLatch.await();
    }

    /**
     * 初始化，最开始的时候的MySQL配置为 mysqlConfig_1
     */
    public static void setMysqlConfig(MysqlConfig config) throws Exception {
        CuratorFramework client = ZKUtils.getClient();
        client.start();
        String mysqlConfigStr = JSON.toJSONString(config);
        Stat s = client.checkExists().forPath(configPath);
        if (s != null) {
            Stat resultStat = client.setData().forPath(configPath, mysqlConfigStr.getBytes());
            System.out.println(String.format("节点 %s 已存在，更新数据为：%s", configPath, mysqlConfigStr));
        } else {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(configPath, mysqlConfigStr.getBytes());
            System.out.println(String.format("创建节点：%s，初始化数据为：%s", configPath, mysqlConfigStr));
        }
        System.out.println();
        client.close();
    }

    /**
     * 通过配置信息，查询MySQL数据库
     */
    public static synchronized void queryMysql(String clientName, MysqlConfig mysqlConfig) throws ClassNotFoundException, SQLException {
        System.out.println(clientName + " 查询MySQL数据，使用的MySQL配置信息：" + mysqlConfig);
        Class.forName(mysqlConfig.getDriver());
        Connection connection = DriverManager.getConnection(mysqlConfig.getUrl(), mysqlConfig.getUsername(), mysqlConfig.getPassword());
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test1");
        while (resultSet.next()) {
            System.out.println(String.format("id=%s, name=%s, age=%s", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
        }
        System.out.println();
        resultSet.close();
        statement.close();
        connection.close();
    }
}
