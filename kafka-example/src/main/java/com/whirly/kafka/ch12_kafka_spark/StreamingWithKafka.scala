package com.whirly.kafka.ch12_kafka_spark
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * @description: ${description}
  * @author: 赖键锋
  * @create: 2019-04-19 21:33
  **/
object StreamingWithKafka {
  private val brokers = "localhost:9092"
  private val topic = "topic-spark"
  private val group = "group-spark"
  private val checkpointDir = "/opt/kafka/checkpoint"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("StreamingWithKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDir)

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](List(topic), kafkaParams))

    // start from assigned offsets.
    //    val partitions = List(new TopicPartition(topic,0),
    //      new TopicPartition(topic,1),
    //      new TopicPartition(topic,2),
    //      new TopicPartition(topic,3))
    //    val fromOffsets = partitions.map(partition => {
    //      partition -> 5000L
    //    }).toMap
    //    val stream = KafkaUtils.createDirectStream[String, String](
    //      ssc, PreferConsistent,
    //      Subscribe[String, String](List(topic), kafkaParams,fromOffsets))

    //    //SubscribePattern.
    //    val stream = KafkaUtils.createDirectStream[String,String](
    //      ssc, PreferConsistent,
    //      SubscribePattern[String,String](Pattern.compile("topic-.*"),kafkaParams)
    //    )

    //Assign
    //    val partitions = List(new TopicPartition(topic,0),
    //      new TopicPartition(topic,1),
    //      new TopicPartition(topic,2),
    //      new TopicPartition(topic,3))
    //    val stream = KafkaUtils.createDirectStream[String,String](
    //      ssc, PreferConsistent,
    //      Assign[String, String](partitions, kafkaParams))

    //Obtaining Offsets
    //    stream.foreachRDD(rdd=>{
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition{iter=>
    //        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    //        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    //      }
    //    })

    val value = stream.map(record => {
      val intVal = Integer.valueOf(record.value())
      println(intVal)
      intVal
    }).reduce(_+_)

    //Window
    //    val value = stream.map(record=>{
    //      Integer.valueOf(record.value())
    //    }).reduceByWindow(_+_, _-_,Seconds(20),Seconds(2))

    value.print()

    ssc.start
    ssc.awaitTermination
  }
}

