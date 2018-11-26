## MapReduce 实验 - 计算气温 最大/最小/平均 值

> 本实验来自于 《Hadoop权威指南》第4班 第2章

美国国家气候数据中心-气象数据集下载地址：ftp://ftp.ncdc.noaa.gov/pub/data/noaa 

### 步骤

#### 1、 下载数据

由于全部数据非常庞大，这里只下载2017年的部分数据用于实验：

```
wget ftp://ftp.ncdc.noaa.gov/pub/data/noaa/2017 -r
```

最正确的数据格式请参考网站

![数据格式](http://image.laijianfeng.org/20181126_162421.png)

#### 2、 合并数据集

由于数据集是由非常多的小文件组成，通常情况下Hadoop处理少量的大型文件更容易、更有效，所以我们将用于实验的数据文件拼接成一个大文件


```
zcat *.gz > coaa.sample.txt
```

#### 3、 上传数据集到HDFS上

```
hadoop dfs -mkdir -p /hadoop/ch2
hadoop dfs -copyFromLocal coaa.sample.txt /hadoop/ch2
```

#### 4、 编写MapReduce程序

分别编写求最大值、最小值、平均值的MapReduce程序

```$xslt
package max;

import common.TemperatureMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxTemperature {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("MapReduce实验-气象数据集-求气温最大值");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TemperatureMapper.class);
        // 设置 Combiner 减少数据的传输量、提高效率
        // job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```

#### 5、 打包上传并运行作业

mvn package 打包
使用 rlzsz 将Jar包上传到服务器

运行三个作业

```
hadoop jar ch2.noaa-1.0-SNAPSHOT.jar max.MaxTemperature /hadoop/ch2/coaa.sample.txt /hadoop/ch2/output/coaa.sample/max
hadoop jar ch2.noaa-1.0-SNAPSHOT.jar min.MinTemperature /hadoop/ch2/coaa.sample.txt /hadoop/ch2/output/coaa.sample/min
hadoop jar ch2.noaa-1.0-SNAPSHOT.jar avg.AvgTemperature /hadoop/ch2/coaa.sample.txt /hadoop/ch2/output/coaa.sample/avg
```

#### 6、 查看运行结果

```
hadoop dfs -cat /hadoop/ch2/output/coaa.sample/max/part-r-00000
hadoop dfs -cat /hadoop/ch2/output/coaa.sample/min/part-r-00000
hadoop dfs -cat /hadoop/ch2/output/coaa.sample/avg/part-r-00000
```


最大值：
2017	302

最小值：
2017	-424

平均值：
2017	49

#### 7、 查看作业状态

http://ip:8088/cluster

![](http://image.laijianfeng.org/20181126_164354.png)


http://ip:50070/explorer.html#/hadoop/ch2/output/coaa.sample

![](http://image.laijianfeng.org/20181126_164609.png)


### 补充

#### 设置 Combiner 可减少数据的传输量、提高效率

```
job.setCombinerClass(MaxTemperatureReducer.class);
```



#### 关于 Hadoop集群加入新节点

* 当Hadoop集群有新节点加入时，正在运行的MapReduce作业自动识别并使用新节点；
* 当新节点加入Hadoop集群后，再启动MapReduce作业，MapReduce作业也能自动识别并使用新节点



