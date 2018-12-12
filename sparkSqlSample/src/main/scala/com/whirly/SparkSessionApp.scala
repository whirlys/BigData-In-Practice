package com.whirly

import org.apache.spark.sql.SparkSession

/**
  * SparkSession 使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    // SparkSession 是 Spark 2.0 统一的入口点

    val spark = SparkSession.builder()
      .appName("SparkSessionApp")
      //.master("local")
      .master("local")
      .getOrCreate();

    val people = spark.read.json("file:///E:/bigdata-workstation/sparkSqlSample/data/people.json")
    people.show()
  }
}
