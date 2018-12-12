package com.whirly

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * DataFrame和RDD的互操作
  * 输出：
  * root
  * |-- id: integer (nullable = true)
  * |-- name: string (nullable = true)
  * |-- age: integer (nullable = true)
  *
  * +---+-------+---+
  * | id|   name|age|
  * +---+-------+---+
  * |  1|Michael| 20|
  * |  2|   Andy| 17|
  * |  3| Justin| 19|
  * +---+-------+---+
  *
  * +---+-------+---+
  * | id|   name|age|
  * +---+-------+---+
  * |  1|Michael| 20|
  * +---+-------+---+
  *
  * +---+-------+---+
  * | id|   name|age|
  * +---+-------+---+
  * |  1|Michael| 20|
  * |  3| Justin| 19|
  * +---+-------+---+
  */
object DataFrameRDDApp {

  // 定义一个 INFO 类
  case class Info(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    println("------------通过反射方式进行互操作-------------")
    inferReflection(spark)

    println("------------通过编程的方式进行互操作-------------")
    program(spark)

    spark.stop()
  }

  /**
    * 使用反射来推断包含了特定数据类型的RDD的元数据
    *
    * @param spark
    */
  def inferReflection(spark: SparkSession) {
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///E:/bigdata-workstation/sparkSqlSample/data/infos.txt")

    //注意：需要导入隐式转换
    import spark.implicits._
    // RDD 通过 toRDD 转换为 DataFrame
    val infoDF = rdd.map(_.split("\t")).map(item => Info(item(0).toInt, item(1), item(2).toInt)).toDF()

    infoDF.printSchema()
    infoDF.show()

    infoDF.filter(infoDF.col("age") > 19).show

    infoDF.createOrReplaceTempView("infos")
    // spark sql 从 infos 中查数据
    spark.sql("select * from infos where age > 18").show()
  }

  /**
    * RDD 与 DataFrame 通过编程的方式进行互操作
    */
  def program(spark: SparkSession): Unit = {
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///E:/bigdata-workstation/sparkSqlSample/data/infos.txt")
    // 注意这里是 Row，且没有 toDF()
    val infoRDD = rdd.map(_.split("\t")).map(item => Row(item(0).toInt, item(1), item(2).toInt))

    // 定义一个 Schema
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()

    infoDF.filter(infoDF.col("age") > 19).show

    infoDF.createOrReplaceTempView("infos")
    // spark sql 从 infos 中查数据
    spark.sql("select * from infos where age > 18").show()
  }

}
