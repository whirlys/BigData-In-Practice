package com.whirly

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 学习 SQLContext 的使用
  *
  * people.printSchema() 打印内容：
  * root
  * |-- age: long (nullable = true)
  * |-- name: string (nullable = true)
  *
  * people.show()打印内容：
  * +----+-------+
  * | age|   name|
  * +----+-------+
  * |null|Michael|
  * |  30|   Andy|
  * |  19| Justin|
  * +----+-------+
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    // SQLContext是 SparkSQL 1.0 版本的入口点，在 2.0 版本中的入口点是 SparkSession

    // 1. 创建相应的 Context
    val conf = new SparkConf()
    conf.setAppName("SparkContextApp")
    //conf.setMaster("spark://master:7077") // spark master URL 或者 local[2] 或者 Yarn 的URL
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)


    // 2. 相关的处理 SQLContext 可以进行的操作，进入类中可查看它的所有方法
    // 譬如我们来处理JSON文件，spark 自带的一些样例文件：$SPARK_HOME/examples/src/main/resources/people.json
    val path = args(0) // IDEA可配置启动参数，在服务器上通过命令行传入
    val people = sqlContext.read.format("json").load(path)
    // 返回一个 DataFrame，DataFrame 数据集也有很多操作，譬如查看它的 schema 信息，查看内容
    people.printSchema()
    people.show()

    // 3. 关闭资源
    sc.stop()
  }
}
