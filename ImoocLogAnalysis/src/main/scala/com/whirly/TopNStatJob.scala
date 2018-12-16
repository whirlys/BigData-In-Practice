package com.whirly

import com.whirly.entity._
import com.whirly.util.StatDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业，需要修改工具类 MySQLUtils 中的数据库连接信息，并创建相应的数据库 imooc_log 和数据表
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    // 分区字段的类型是自动识别的，目前支持numeric和string类型
    // 有时用户不想让它自动识别，可以通过 spark.sql.sources.partitionColumnTypeInference.enabled 配置，
    // 默认为true。禁用之后，分区键总是被当成字符串类型
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val day = "20161110"
    StatDao.deleteData(day) // 删除当天的数据

    // 从 cleaned 后的数据中可以得到的字段 [url、cmsType、cmsId、traffic、ip、city、time、day]
    val accessDF = spark.read.format("parquet").load(Constants.cleanedOut)
    accessDF.cache() // 缓存

    // 统计某天的数据
    runStatJobByDay(spark, accessDF, day, 10)

    spark.stop()
  }

  /**
    * 统计某天的数据
    */
  def runStatJobByDay(spark: SparkSession, accessDF: DataFrame, day: String, N: Int): Unit = {
    // 某天最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day, 10)

    // 统计某天各个省市各自的 TopN 课程
    cityAccessTopNStat(spark, accessDF, day)

    // 按照流量进行统计 TopN 课程
    videoTrafficsTopNStat(spark, accessDF, day, 10)

    // 某天最受欢迎的文章
    articleAccessTopNStat(spark, accessDF, day, 10)

    // 某天进行code最多的课程
    codeAccessTopNStat(spark, accessDF, day, 10)

    //  统计某天最勤奋的 IP
    ipAccessTopNStat(spark, accessDF, day, 10)
  }

  /**
    * 某天最受欢迎的TopN课程：先过滤出当天的video记录，再对课程ID进行分组求和，降序输出
    *
    * @param day 日期字符串 20161110
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String, N: Int): Unit = {
    /**
      * 第一种统计方式：使用DataFrame的方式进行统计，导入隐式转换包
      */
    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= "0")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    // 使用 agg 函数需要 import org.apache.spark.sql.functions._

    /**
      * 第二种统计方式：使用SQL的方式进行统计
      */
    // 建立临时表
    // accessDF.createOrReplaceTempView("access_logs")
    // val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
    //   " where day = " + day + " and cmsType='video' " +
    //   " group by day, cmsId order by times desc")

    videoAccessTopNDF.show(false)

    /** 统计结果示例：
      * +--------+-----+-----+
      * |day     |cmsId|times|
      * +--------+-----+-----+
      * |20161110|2402 |32   |
      * |20161110|1309 |20   |
      * |20161110|3078 |19   |
      * |20161110|2801 |18   |
      * |20161110|4018 |16   |
      * +--------+-----+-----+
      */

    val window = Window.partitionBy(col("day")).orderBy(col("times").desc)
    val topNDF = videoAccessTopNDF.withColumn("topn", row_number().over(window)).where(col("topn") <= N)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      topNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayVideoAccessStat]

        partition.foreach(row => {
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val times = row.getAs[Long]("times")

          // 不建议在此处进行数据库的数据插入，而是先构建list，再通过批量的方式插入MySQL
          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        // 批量插入
        StatDao.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

    /**
      * 查看 MySQL中的 day_video_access_topn_stat 表： SELECT * FROM `day_video_access_topn_stat` ORDER BY times DESC
      * 20161110	2402	32
      * 20161110	1309	20
      * 20161110	3078	19
      * 20161110	2801	18
      * 20161110	4018	16
      * 20161110	1336	16
      * 20161110	3369	16
      * ....
      */
  }

  /**
    * 统计某天各个省市各自的 TopN 课程
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= "0")
      .groupBy("city", "day", "cmsId")
      .agg(count("cmsId").as("times"))

    //    cityAccessTopNDF.show(false)

    // Window 函数在Spark SQL的使用
    // 窗口函数 row_number 的作用是根据表中字段进行分组，然后根据表中的字段排序，给组中的每条记录添
    //     加一个序号；且每组的序号都是从1开始，可利用它的这个特性进行分组取top-n
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city")) // 根据 city 分组，根据 times 降序排序
        .orderBy(cityAccessTopNDF("times").desc))
        .as("times_rank")
    ).filter("times_rank <= 3") //.show(100,false)

    /**
      * +--------+-------+-----+-----+----------+
      * |day     |city   |cmsId|times|times_rank|
      * +--------+-------+-----+-----+----------+
      * |20161110|北京市    |1309 |20   |1         |
      * |20161110|北京市    |3369 |16   |2         |
      * |20161110|北京市    |4018 |15   |3         |
      * |20161110|辽宁省    |1336 |2    |1         |
      * |20161110|辽宁省    |9028 |1    |2         |
      * |20161110|辽宁省    |8141 |1    |3         |
      * |20161110|浙江省    |3078 |19   |1         |
      * |20161110|浙江省    |12552|16   |2         |
      * |20161110|浙江省    |3237 |14   |3         |
      * +--------+-------+-----+-----+----------+
      * .....
      */

    // 保存到 MySQL，需创建结果表 day_video_city_access_topn_stat
    try {
      top3DF.foreachPartition(partition => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partition.foreach(item => {
          val day = item.getAs[String]("day")
          val cmsId = item.getAs[Long]("cmsId")
          val city = item.getAs[String]("city")
          val times = item.getAs[Long]("times")
          val timesRank = item.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDao.insertDayCityVideoAccessTopN(list)

        /**
          * 查看MySQL插入结果：select * from day_video_city_access_topn_stat ORDER BY city asc, times desc
          * //TODO 为什么出现“全球”？MySQL中还出现了4条记录？多了一条 7831？
          * 20161110	2030	上海市	14	1
          * 20161110	13192	上海市	2	2
          * 20161110	2469	上海市	2	3
          * 20161110	7076	全球 	4	2
          * 20161110	7075	全球 	4	1
          * 20161110	11950	全球 	2	3
          * 20161110	7831	全球	  1	1
          * ...
          */
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照流量进行统计 TopN 课程
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String, N: Int): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= "0")
      .groupBy("day", "cmsId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    // .show(false)
    /**
      * +--------+-----+--------+
      * |day     |cmsId|traffics|
      * +--------+-----+--------+
      * |20161110|10506|3145836 |
      * |20161110|5609 |1048643 |
      * |20161110|8175 |1048630 |
      * |20161110|11938|1048576 |
      * |20161110|11724|1048576 |
      * +--------+-----+--------+
      */

    val window = Window.partitionBy(col("day")).orderBy(col("traffics").desc)
    val topNDF = cityAccessTopNDF.withColumn("topn", row_number().over(window)).where(col("topn") <= N)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      topNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]
        partitionOfRecords.foreach(item => {
          val day = item.getAs[String]("day")
          val cmsId = item.getAs[Long]("cmsId")
          val traffics = item.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        }
        )
        StatDao.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 某天访问量最高的文章
    */
  def articleAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String, N: Int): Unit = {
    import spark.implicits._
    val articleAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "article" && $"cmsId" =!= "0")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    articleAccessTopNDF.printSchema()
    articleAccessTopNDF.show(false)

    val window = Window.partitionBy(col("day")).orderBy(col("times").desc)
    val topNDF = articleAccessTopNDF.withColumn("topn", row_number().over(window)).where(col("topn") <= N)

    // 将统计结果写入到MySQL中
    try {
      topNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayArticleAccessStat]
        partition.foreach(row => {
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val times = row.getAs[Long]("times")
          list.append(DayArticleAccessStat(day, cmsId, times))
        })
        StatDao.insertDayArticleAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 某天进行code最多的课程
    */
  def codeAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String, N: Int): Unit = {
    import spark.implicits._
    val codeAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "code" && $"cmsId" =!= "0")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    codeAccessTopNDF.printSchema()
    codeAccessTopNDF.show(false)

    /**
      * +--------+-----+-----+
      * |day     |cmsId|times|
      * +--------+-----+-----+
      * |20161110|738  |21   |
      * |20161110|2053 |20   |
      * |20161110|2208 |19   |
      * +--------+-----+-----+
      */

    val window = Window.partitionBy(col("day")).orderBy(col("times").desc)
    val topNDF = codeAccessTopNDF.withColumn("topn", row_number().over(window)).where(col("topn") <= N)

    try {
      topNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayCodeAccessStat]
        partition.foreach(row => {
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val times = row.getAs[Long]("times")
          list.append(DayCodeAccessStat(day, cmsId, times))
        })
        StatDao.insertDayCodeAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 某天最勤奋的 ip
    */
  def ipAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String, N: Int): Unit = {
    import spark.implicits._
    val ipAccessTopNDF = accessDF.filter($"day" === day && $"cmsId" =!= "0")
      .groupBy("day", "ip").agg(count("ip").as("times")).orderBy($"times".desc)

    val window = Window.partitionBy(col("day")).orderBy(col("times").desc)
    val topNDF = ipAccessTopNDF.withColumn("topn", row_number().over(window)).where(col("topn") <= N)

    topNDF.show(false)

    /**
      * +--------+---------------+-----+----+
      * |day     |ip             |times|topn|
      * +--------+---------------+-----+----+
      * |20161110|115.34.187.133 |47   |1   |
      * |20161110|119.131.143.179|44   |2   |
      * |20161110|114.248.224.214|30   |3   |
      * |20161110|119.130.175.132|29   |4   |
      * |20161110|58.217.142.142 |29   |5   |
      * |20161110|27.46.226.69   |28   |6   |
      * |20161110|157.0.78.103   |27   |7   |
      * |20161110|14.219.128.52  |25   |8   |
      * |20161110|120.198.231.151|24   |9   |
      * |20161110|211.162.33.31  |22   |10  |
      * +--------+---------------+-----+----+
      */
    try {
      topNDF.foreachPartition(partition => {
        val list = new ListBuffer[DayIpAccessStat]
        partition.foreach(row => {
          val day = row.getAs[String]("day")
          val ip = row.getAs[String]("ip")
          val times = row.getAs[Long]("times")
          list.append(DayIpAccessStat(day, ip, times))
        })
        StatDao.insertDayIpAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
