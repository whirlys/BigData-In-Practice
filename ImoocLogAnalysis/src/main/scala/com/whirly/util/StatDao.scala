package com.whirly.util

import java.sql.{Connection, PreparedStatement}

import com.whirly.entity._

import scala.collection.mutable.ListBuffer

/**
  * 将各个统计信息写入MySQL
  */
object StatDao {


  /**
    * 批量保存DayVideoAccessStat到数据库
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 不要自动提交，设置为手动提交

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setLong(2, item.cmsId)
        pstmt.setLong(3, item.times)

        pstmt.addBatch() // 添加到批量处理
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      MySQLUtils.release(connection, pstmt) // 释放连接
    }
  }


  /**
    * 批量保存 DayCityVideoAccessStat 到数据库
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setLong(2, item.cmsId)
        pstmt.setString(3, item.city)
        pstmt.setLong(4, item.times)
        pstmt.setInt(5, item.timesRank)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }


  /**
    * 批量保存 DayVideoTrafficsStat 到数据库
    */
  def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setLong(2, item.cmsId)
        pstmt.setLong(3, item.traffics)
        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }


  /**
    * 删除指定日期的数据
    */
  def deleteData(day: String): Unit = {
    val tables = Array("day_video_access_topn_stat",
      "day_video_city_access_topn_stat",
      "day_video_traffics_topn_stat",
      "day_article_access_topn_stat",
      "day_code_access_topn_stat",
      "day_ip_access_topn_stat")

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      for (table <- tables) {
        // delete from table ....
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }


  /**
    * 某天访问量最高的文章
    */
  def insertDayArticleAccessTopN(list: ListBuffer[DayArticleAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 不要自动提交，设置为手动提交

      val sql = "insert into day_article_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setLong(2, item.cmsId)
        pstmt.setLong(3, item.times)

        pstmt.addBatch() // 添加到批量处理
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      MySQLUtils.release(connection, pstmt) // 释放连接
    }
  }

  /**
    * 某天进行code最多的课程
    */
  def insertDayCodeAccessTopN(list: ListBuffer[DayCodeAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 不要自动提交，设置为手动提交

      val sql = "insert into day_code_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setLong(2, item.cmsId)
        pstmt.setLong(3, item.times)

        pstmt.addBatch() // 添加到批量处理
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      MySQLUtils.release(connection, pstmt) // 释放连接
    }
  }


  /**
    * 插入 某天最勤奋的 IP 统计结果
    *
    * @param list
    */
  def insertDayIpAccessTopN(list: ListBuffer[DayIpAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 不要自动提交，设置为手动提交

      val sql = "insert into day_ip_access_topn_stat(day,ip,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (item <- list) {
        pstmt.setString(1, item.day)
        pstmt.setString(2, item.ip)
        pstmt.setLong(3, item.times)
        pstmt.addBatch() // 添加到批量处理
      }
      pstmt.executeBatch() // 执行批量处理
      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      MySQLUtils.release(connection, pstmt) // 释放连接
    }
  }

  def main(args: Array[String]): Unit = {
    deleteData("20161110")
  }
}
