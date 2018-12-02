## 自定义分区

> 实验来源：[hadoop MapReduce自定义分区partition的作用和用法](https://blog.csdn.net/wo198711203217/article/details/80621738)

把各个部门的数据分发到各自的reduce task上

```$xslt
hdfs dfs -mkdir -p /hadoop/cus-partition/input
hdfs dfs -put jidu1.txt /hadoop/cus-partition/input
hdfs dfs -put jidu2.txt /hadoop/cus-partition/input
hdfs dfs -put jidu3.txt /hadoop/cus-partition/input
hdfs dfs -put jidu4.txt /hadoop/cus-partition/input

# 运行
hadoop jar customize-Partition-1.0-SNAPSHOT.jar JiduRunner /hadoop/cus-partition/input /hadoop/cus-partition/output
```

#### 测试数据

```$xslt
# cat jidu1.txt 
研发部门        100
测试部门        90
硬件部门        92
销售部门        200

# cat jidu2.txt 
研发部门        200
测试部门        93
硬件部门        95
销售部门        230

# cat jidu3.txt 
研发部门        202
测试部门        92
硬件部门        94
销售部门        231

# cat jidu4.txt 
研发部门        209
测试部门        98
硬件部门        99
销售部门        251
```

#### 结果汇总

```$xslt
研发部门	711
测试部门	373
硬件部门	380
销售部门	912
```