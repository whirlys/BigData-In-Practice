# BigData-In-Practice
大数据项目仓库、涉及 Hadoop、Spark、Kafka、Hbase..... 等，更新中...



### 综合实践项目

| 项目名                                                       | 说明                                                  |
| ------------------------------------------------------------ | ----------------------------------------------------- |
| [ImoocLogAnalysis](https://github.com/whirlys/BigData-In-Practice/tree/master/ImoocLogAnalysis) | 使用 Spark SQL imooc 访问日志，数据清洗，统计，可视化 |



### 入门学习示例

| 项目名                                                       | 所属组件  | 介绍                                                         |
| ------------------------------------------------------------ | --------- | ------------------------------------------------------------ |
| [ch2noaa](https://github.com/whirlys/BigData-In-Practice/tree/master/ch2noaa) | MapReduce | MapReduce 实验 - 计算气温 最大/最小/平均 值                  |
| [hdfscrud](https://github.com/whirlys/BigData-In-Practice/tree/master/hdfscrud) | HDFS      | HDFS Java API 增删查改                                       |
| [hdfslogcollect](https://github.com/whirlys/BigData-In-Practice/tree/master/hdfslogcollect) | HDFS      | Timer 定时将日志文件备份到 HDFS 中去，copyFromLocalFile      |
| [commonfans](https://github.com/whirlys/BigData-In-Practice/tree/master/commonfans) | MapReduce | MapReduce 计算共同好友                                       |
| [hadoopjoin](https://github.com/whirlys/BigData-In-Practice/tree/master/hadoopjoin) | MapReduce | MapReduce 两表进行左连接 left join，两表进行 map join        |
| [customizePartition](https://github.com/whirlys/BigData-In-Practice/tree/master/customizePartition) | MapReduce | MapReduce 自定义分区 ，Partitioner 决定每条记录应该送往哪个reducer节点 |
| [sparkSqlSample](https://github.com/whirlys/BigData-In-Practice/tree/master/sparkSqlSample) | Spark SQL | Spark SQL 样例，关于HiveContext、SQLContext、SparkSession、RDD、DataFrame、Dataset的使用 |
| [curator-example](https://github.com/whirlys/BigData-In-Practice/tree/master/curator-example) | Zookeeper | 基于Apache Curator实现对Zookeeper的操作，以及数据发布/订阅、负载均衡、命名服务、分布式协调/通知、集群管理、Master选举、分布式锁和分布式队列等Zookeeper的应用场景 |
| [HbaseExamples](https://github.com/whirlys/BigData-In-Practice/tree/master/HbaseExamples) | HBase     | Hbase Java API的基本操作，包括增删查改、过滤器、协处理器，Phoenix、Phoenix+Mybatis等 |
| [kafka-example](https://github.com/whirlys/BigData-In-Practice/tree/master/kafka-example) | Kafka     | 深入理解Kafka各种操作，生产者、消费者、主题、分区、应用、可靠性、spark |



### 数据算法 Hadoop + Spark 实现



### 阅读资料

#### Canal

- [深入解析中间件之-Canal](https://zqhxuyuan.github.io/2017/10/10/Midd-canal/#)

- [田守技 - canal源码解析 - 1.0 canal源码分析简介](http://www.tianshouzhi.com/api/tutorials/canal/380)
- [田守技 - canal源码解析 - 2.0 deployer模块](http://www.tianshouzhi.com/api/tutorials/canal/381)
- [田守技 - canal源码解析 - 3.0 server模块](http://www.tianshouzhi.com/api/tutorials/canal/382)
- [田守技 - canal源码解析 - 4.0 instance模块](http://www.tianshouzhi.com/api/tutorials/canal/391)
- [田守技 - canal源码解析 - 5.0 store模块](http://www.tianshouzhi.com/api/tutorials/canal/401)
- [田守技 - canal源码解析 - 6.0 filter模块](http://www.tianshouzhi.com/api/tutorials/canal/402)
- [田守技 - canal源码解析 - 7.0 driver模块](http://www.tianshouzhi.com/api/tutorials/canal/403)
- [田守技 - canal源码解析 - 8.0 异地多活场景下的数据同步之道](http://www.tianshouzhi.com/api/tutorials/canal/404)