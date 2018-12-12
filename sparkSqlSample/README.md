
## Spark SQL 样例

- 学习 HiveContext 的使用，对 Hive 进行增查join
- 使用外部数据源综合查询Hive和MySQL的表数据
- SQLContext 的使用
- SparkSession 使用
- JDBC 连接 ThriftServer 进行查询
- DataFrame API基本操作
- DataFrame和RDD的互操作
- Dataset操作

### 环境

- Java： 1.8
- maven
- spark： 2.3.1
- Scala： 2.11.12


### 作业部署的方式

文档：http://spark.apachecn.org/#/docs/15

#### 在 Windows 上本地运行

需要下载Hadoop和Spark，然后解压到某个路径下，在环境变量中配置 HADOOP_HOME 和 SPARK_HOME，然后下载 [winutils.exe](https://github.com/srccodes/hadoop-common-2.2.0-bin/tree/master/bin) 放到 ${HADOOP_HOME}/bin 下 


#### 提交到 Spark 运行

打 Jar 包后上传到服务器，提交到spark命令：
```$xslt
spark-submit --class "com.whirly.SparkContextApp" sparkSqlSample.jar file:///sda/bigdata/app/spark/examples/src/main/resources/people.json
spark-submit --class com.whirly.HiveContextApp --master spark://master:7077 sparkSqlSample.jar file:///home/whirly/spark/people.txt file:///home/whirly/spark/peopleScore.txt
```


HiveContext 因为元数据存在MySQL中，所以启动时需要把MySQL驱动加入classpath路径中






spark-shell  spark-sql hive 操作
