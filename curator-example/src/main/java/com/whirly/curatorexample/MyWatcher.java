package com.whirly.curatorexample;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @program: curator-example
 * @description: zk原生API的Watcher接口实现
 * @author: 赖键锋
 * @create: 2019-01-18 17:10
 **/
public class MyWatcher implements Watcher {
    // Watcher事件通知方法
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("触发watcher事件：" + watchedEvent);
    }
}
