package com.whirly

import org.apache.spark.sql.SparkSession

/**
  * Dataset操作
  */
object DatasetApp {

  // 定义people类
  case class People(name: String, age: Int, job: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    //注意：需要导入隐式转换
    import spark.implicits._

    val path = "file:///E:/bigdata-workstation/sparkSqlSample/data/people.csv"
    // spark 如何解析csv文件？
    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show

    // DataFrame 转为 DataSet
    val ds = df.as[People]
    ds.map(line => line.name).show

    spark.stop()
  }
}
