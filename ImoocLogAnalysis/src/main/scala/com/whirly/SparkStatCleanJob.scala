package com.whirly

import com.whirly.util.AccessConvertUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    // 删除结果目录
    // DirUtil.deleteDir(Constants.cleanedOut)

    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile(Constants.protocol + Constants.cleanedIn)

    // accessRDD.take(10).foreach(println)

    val filterRDD = accessRDD.map(line => AccessConvertUtil.parseLog(line))
    //.filter(row => !"全球".equals(row.getAs[String](5)))

    //RDD ==> DataFrame
    val accessDF = spark.createDataFrame(filterRDD, AccessConvertUtil.struct)
      // 过滤掉city为 全球 的记录  过滤不干净怎么回事？
      //.filter($"city" =!= "全球")
      //.where(col("city") =!= "全球")

    accessDF.printSchema()
    accessDF.show(false)

    // 保存到 CSV，方便查看结果，与 parquet 比较
    accessDF.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).partitionBy("day").save("file:///F:/data/imooc-log/cleanedCsv")
    // 保存到 parquet  测试数据2464条，  CSV ===> 199K      parquet ===> 30K   可见 parquet 的优势
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(Constants.protocol + Constants.cleanedOut)

    // 保存到mysql
//    accessDF.write.format("jdbc")
//      .mode(SaveMode.Overwrite)
//      .option("url", "jdbc:mysql://master:3306").option("dbtable", "imooc_log.imooc_access_log")
//      .option("user", "root").option("password", "123456").save()
    spark.stop()
  }

  /**
    * +--------------------------------------------+-------+-----+-------+---------------+----+-------------------+--------+
    * |url                                         |cmsType|cmsId|traffic|ip             |city|time               |day     |
    * +--------------------------------------------+-------+-----+-------+---------------+----+-------------------+--------+
    * |http://www.imooc.com/code/1852              |code   |1852 |2345   |117.35.88.11   |陕西省 |2016-11-10 00:01:02|20161110|
    * |http://www.imooc.com/learn/85/?src=360onebox|learn  |85   |14531  |115.34.187.133 |北京市 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/course/list?c=fetool   |course |0    |66     |120.198.231.151|广东省 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/code/10047             |code   |10047|54     |101.36.73.155  |北京市 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/article/publish        |article|0    |2957   |111.73.157.155 |江西省 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/video/12482            |video  |12482|54     |61.142.10.231  |广东省 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/code/315               |code   |315  |54     |58.211.49.82   |江苏省 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/video/6689             |video  |6689 |320    |14.153.236.58  |广东省 |2016-11-10 00:01:27|20161110|
    * |http://www.imooc.com/video/6689             |video  |6689 |4166   |14.153.236.58  |广东省 |2016-11-10 00:01:27|20161110|
    * +--------------------------------------------+-------+-----+-------+---------------+----+-------------------+--------+
    */
}
