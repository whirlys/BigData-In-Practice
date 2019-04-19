package com.whirly.kafka.ch12_kafka_spark

import org.apache.spark.sql.SparkSession
/**
  * @description: ${description}
  * @author: 赖键锋
  * @create: 2019-04-19 21:37
  **/
object StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]") .appName("StructuredStreamingWordCount") .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}

