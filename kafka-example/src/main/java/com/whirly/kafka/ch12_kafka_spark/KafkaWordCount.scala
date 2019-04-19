package com.whirly.kafka.ch12_kafka_spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @description: ${description}
  * @author: 赖键锋
  * @create: 2019-04-19 21:31
  **/
object KafkaWordCount {
  private val brokers = "localhost:9092"
  private val topic = "topic-spark"
  private val group = "group-spark"
  private val checkpointDir = "/opt/kafka/checkpoint"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDir)

    val kafkaParam = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](List(topic), kafkaParam))

    val words = stream.map(record => record.value()).flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(2), 2)
    wordCount.print

    ssc.start
    ssc.awaitTermination
  }
}
