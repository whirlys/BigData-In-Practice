## Hadoop 用 MapReduce 实现 left join 操作

> left join案例来源：[hadoop 用MR实现join操作](https://blog.csdn.net/bitcarmanlee/article/details/51863358)    
> 技术点参考：[Map Reduce进阶编程案例-信贷数据分析案例](https://zhuanlan.zhihu.com/p/50826582)   
> map join案例来源：[MapReduce表连接操作之Map端join](https://blog.csdn.net/lzm1340458776/article/details/42971075) 

------

### hadoop 用MR实现join操作
#### 输入

employee.txt
```$xslt
jd,david
jd,mike
tb,mike
tb,lucifer
elong,xiaoming
elong,ali
tengxun,xiaoming
tengxun,lilei
xxx,aaa
```

salary.txt
```$xslt
jd,1600
tb,1800
elong,2000
tengxun,2200
```

#### 运行

```$xslt
hadoop jar hadoop-join-1.0-SNAPSHOT.jar LeftJoin \
    -Dinput_dir=/hadoop/join/input \
    -Doutput_dir=/hadoop/join/output \
    -Dmapred.textoutputformat.separator=","
```

#### 输出

```$xslt
jd,mike,1600
jd,david,1600
tb,lucifer,1800
tb,mike,1800
xxx,aaa,null
elong,ali,2000
elong,xiaoming,2000
tengxun,lilei,2200
tengxun,xiaoming,2200
```

-----

### MapReduce表连接操作之Map端join

tba.txt

```$xslt

zhang	male	20	1
li	female	25	2
wang	female	30	3
zhou	male	35	2
```

tbb.txt

```$xslt
1	sales
2	Dev
3	Mgt
```

#### 运行

```
hdfs dfs -mkdir -p /hadoop/mapjoin/input
hdfs dfs -mkdir -p /hadoop/mapjoin/cache
hdfs dfs -mkdir -p /hadoop/mapjoin/input
hdfs dfs -put tbb.txt /hadoop/mapjoin/cache

# 运行作业
hadoop jar hadoop-join-1.0-SNAPSHOT.jar MapJoin /hadoop/mapjoin/input /hadoop/mapjoin/cache/tbb.txt /hadoop/mapjoin/output
```

#### 运行结果

```$xslt
Emp_Dep{name='zhang', sex='male', age=20, depNo=1, depName='sales'}
Emp_Dep{name='li', sex='female', age=25, depNo=2, depName='Dev'}
Emp_Dep{name='wang', sex='female', age=30, depNo=3, depName='Mgt'}
Emp_Dep{name='zhou', sex='male', age=35, depNo=2, depName='Dev'}
```