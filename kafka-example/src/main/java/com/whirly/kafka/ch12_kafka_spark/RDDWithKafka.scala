package com.whirly.kafka.ch12_kafka_spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions

/**
  * @description: ${description}
  * @author: 赖键锋
  * @create: 2019-04-19 21:33
  **/
object RDDWithKafka {
  private val brokers = "localhost:9092"
  private val topic = "topic-spark"
  private val group = "group-spark-rdd"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDDWithKafka")
    val ssc = new SparkContext(sparkConf)

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group
    )

    val offsetRanges = Array(
      OffsetRange(topic, 0, 0, 100),
      OffsetRange(topic, 1, 0, 100),
      OffsetRange(topic, 2, 0, 100),
      OffsetRange(topic, 3, 0, 100)
    )
    val rdd = KafkaUtils.createRDD(ssc,
      JavaConversions.mapAsJavaMap(kafkaParams),
      offsetRanges, PreferConsistent)
    rdd.foreachPartition(records => {
      records.foreach(record => {
        println(record.topic() + ":" + record.partition() + ":" + record.value())
      })
    })
  }
}
