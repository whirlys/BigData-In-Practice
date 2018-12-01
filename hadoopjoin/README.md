## Hadoop 用 MapReduce 实现 left join 操作

> 案例来源：[hadoop 用MR实现join操作](https://blog.csdn.net/bitcarmanlee/article/details/51863358)    
> 技术点参考：[Map Reduce进阶编程案例-信贷数据分析案例](https://zhuanlan.zhihu.com/p/50826582)

### 输入

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

### 运行

```$xslt
hadoop jar hadoop-join-1.0-SNAPSHOT.jar LeftJoin \
    -Dinput_dir=/hadoop/join/input \
    -Doutput_dir=/hadoop/join/output \
    -Dmapred.textoutputformat.separator=","
```

### 输出

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