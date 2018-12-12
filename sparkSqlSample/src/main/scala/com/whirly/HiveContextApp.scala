package com.whirly

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 学习 HiveContext 的使用，对 Hive 进行增查join
  * dataframeHive.show() 输出：
  * +-------+---+-----+
  * |   name|age|score|
  * +-------+---+-----+
  * |Michael| 20|   98|
  * |   Andy| 17|   95|
  * +-------+---+-----+
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // HiveContext 继承自 SQLContext，也是 SparkSQL 1.0 版本的入口点
    val peoplePath = args(0) // people.txt 路径
    println("people.txt 路径：" + peoplePath)
    val peopleScorePath = args(1) // peopleScore.txt 路径
    println("peopleScore.txt 路径：" + peopleScorePath)


    // 1. 创建相应的 Context
    val conf = new SparkConf()
    conf.setAppName("HiveContextApp")
    val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)

    // 2. 创建表结构，有两个表，people(name, age) ，peopleScore(name, score)
    hiveContext.sql("use default")
    hiveContext.sql("drop table if exists people")
    hiveContext.sql("create table if not exists people(name  STRING, age INT) row format delimited fields terminated by '\\t' lines terminated by '\\n'")

    // 3. 加载数据到表中， 这里加载本地数据，两个文件路径从命令参数获取
    hiveContext.sql("load data local inpath '%s' into table people".format(peoplePath))

    hiveContext.sql("drop table if exists peopleScores")
    hiveContext.sql("create table if not exists peopleScores  (name  STRING, score INT) row format delimited fields terminated by '\\t' lines terminated by '\\n'")

    hiveContext.sql("load data local inpath '%s' into table peopleScores".format(peopleScorePath))

    // 4. 通过 HiveContext 使用 join 对 Hive 中的两个表进行连表操作
    val resultDF = hiveContext.sql("select p.name, p.age, ps.score from people p join peopleScores ps on p.name=ps.name where ps.score > 90")

    // 5. 通过 saveAsTable 创建一张hive managed table，数据的元数据和数据即将放的具体位置都是由 hive 数据仓库进行管理的，当删除该表的时候，数据也会一起被删除（磁盘的数据不再存在）
    hiveContext.sql("drop table if exists peopleResult")
    resultDF.write.saveAsTable("peopleResult")

    // 6. 使用HiveContext的table方法可以直接读取hive数据仓库的Table并生成DataFrame,  接下来可机器学习、图计算、各种复杂的ETL等操作
    val dataframeHive = hiveContext.table("peopleResult")
    dataframeHive.show()  // 展示

    // 7. 关闭资源
    sc.stop()
  }
}
