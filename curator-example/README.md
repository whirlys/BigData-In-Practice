# Zookeeper 学习示例

Zookeeper 是一个高可用的分布式数据管理与协调框架，基于ZAB协议算法的实现，该框架能够很好的保证分布式环境中数据的一致性。Zookeeper的典型应用场景主要有：数据发布/订阅、负载均衡、命名服务、分布式协调/通知、集群管理、Master选举、分布式锁和分布式队列等。



Apache Curator是一个Zookeeper的开源客户端，它提供了Zookeeper各种应用场景（Recipe，如共享锁服务、master选举、分布式计数器等）的抽象封装，本文使用 Curator 提供的Recipe来实现Master选举。





