package com.whirly

import com.whirly.util.{DateUtils, DirUtil}
import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    // 删除结果目录
    DirUtil.deleteDir(Constants.tempOut)

    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    // 获取 日志文件 access.1w.log RDD
    val accessFile = spark.sparkContext.textFile(Constants.protocol + Constants.rawPath)

    // accessFile.take(10).foreach(println)

    // 日志格式：
    // 183.162.52.7 - - [10/Nov/2016:00:01:02 +0800] "POST /api3/getadv HTTP/1.1" 200 813 "www.imooc.com" "-"
    // cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96
    // "mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G" "-" 10.100.134.244:80 200 0.027 0.027

    // val splits = line.split(" ") 结果：

    // 0  => 183.162.52.7
    // 1,2 => -
    // 3  => [10/Nov/2016:00:01:02
    // 4  => +0800]
    // 5  => "POST
    // 6  => /api3/getadv
    // 7  => HTTP/1.1"
    // 8  => 200
    // 9  => 813
    // 10 => "www.imooc.com"
    // 11 => "-"
    // 12 => cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96
    // 13 => "mukewang/5.0.0
    // 14 => (Android
    // 15 => 5.1.1;
    // 16 => Xiaomi
    // 17 => Redmi
    // 18 => 3
    // 19 => Build/LMY47V),Network
    // 20 => 2G/3G"
    // 22 => 10.100.134.244:80
    // 23 => 200
    // 24 => 0.027
    // 25 => 0.027

    accessFile.map(line => {
      val splits = line.split(" ") // 按空格分割

      val ip = splits(0) // 第一个是IP

      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
        * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
        */
      val time = splits(3) + " " + splits(4)

      val url = splits(11).replaceAll("\"", "") // 第11个是 URL
      val traffic = splits(9) // 第9个是流量

      List(DateUtils.parse(time), url, traffic, ip)
    })
      // 过滤掉 ip = 10.100.0.1 的记录
      .filter(item => !"10.100.0.1".equals(item(3)))
      // 过滤掉 url 为 - 的记录
      .filter(item => !"-".equals(item(1)))
      // 拼成一个对象返回：(DateUtils.parse(time), url, traffic, ip)
      .map(item => item(0) + "\t" + item(1) + "\t" + item(2) + "\t" + item(3))
      // 保存
      .saveAsTextFile(Constants.protocol + Constants.tempOut)

    // 得到一些结果
    /**
      * 2016-11-10 00:01:27	http://www.imooc.com/code/10047	54	101.36.73.155
      * 2016-11-10 00:01:27	http://www.imooc.com/article/publish	2957	111.73.157.155
      * 2016-11-10 00:01:27	http://www.imooc.com/video/12482	54	61.142.10.231
      * 2016-11-10 00:01:27	http://www.imooc.com/code/315	54	58.211.49.82
      * 2016-11-10 00:01:27	http://www.imooc.com/video/6689	320	14.153.236.58
      * 2016-11-10 00:01:27	http://www.imooc.com/video/6689	4166	14.153.236.58
      */

    spark.stop()
  }
}
