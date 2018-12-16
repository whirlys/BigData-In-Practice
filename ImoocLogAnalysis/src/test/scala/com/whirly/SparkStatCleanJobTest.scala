package com.whirly

import org.apache.spark.sql.SparkSession

object SparkStatCleanJobTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]").getOrCreate()

//    val csvDF = spark.read.csv("file:///F:/data/imooc-log/cleanedCsv")
//    csvDF.printSchema()
//    csvDF.foreach(row => println(row))

    val parquetDF = spark.read.parquet(Constants.protocol + Constants.cleanedOut)
    parquetDF.printSchema()
    parquetDF.foreach(row => println(row))

    spark.stop()
  }
}
