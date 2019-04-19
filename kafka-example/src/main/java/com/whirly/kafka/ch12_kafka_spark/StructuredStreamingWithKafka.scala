package com.whirly.kafka.ch12_kafka_spark
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession
/**
  * @description: ${description}
  * @author: 赖键锋
  * @create: 2019-04-19 21:34
  **/
class StructuredStreamingWithKafka {
  object StructuredStreamingWithKafka {
    val brokerList = "localhost:9092" //Kafka 集群的地址
    val topic = "topic-spark" //订阅的主题
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder.master("local[2]")
        .appName("StructuredStreamingWithKafka").getOrCreate()

      import spark.implicits._

      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",brokerList)
        .option("subscribe",topic)
        .load()

      val ds = df.selectExpr("CAST(value AS STRING)").as[String]

      val words=ds.flatMap(_.split("")).groupBy("value").count()
      val query = words.writeStream
        .outputMode("complete")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .format("console")
        .start()
      query.awaitTermination()
    } }
}
