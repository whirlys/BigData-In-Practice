package com.whirly.curatorexample;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * @program: curator-example
 * @description: Curator提供的CuratorWatcher接口实现
 * @author: 赖键锋
 * @create: 2019-01-18 17:12
 **/
public class MyCuratorWatcher implements CuratorWatcher {
    // Watcher事件通知方法
    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        System.out.println("触发watcher事件：" + watchedEvent);
    }
}
