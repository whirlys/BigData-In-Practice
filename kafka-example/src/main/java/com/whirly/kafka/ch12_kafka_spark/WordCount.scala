package com.whirly.kafka.ch12_kafka_spark
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @description: ${description}
  * @author: 赖键锋
  * @create: 2019-04-19 21:38
  **/
object WordCount {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/opt/spark/bin/spark-shell")
    val wordcount = rdd.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    val wordsort = wordcount.map(x=>(x._2,x._1))
      .sortByKey(false).map(x=>(x._2,x._1))
    wordsort.saveAsTextFile("/tmp/spark")
    sc.stop()
  }
}