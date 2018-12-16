package com.whirly.util

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL操作工具类
  */
object MySQLUtils {
  /**
    * 获取数据库连接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://master:3306/imooc_log?user=root&password=123456")
  }

  /**
    * 释放数据库连接等资源
    */
  def release(connection: Connection, psmt: PreparedStatement) = {
    try {
      if (psmt != null) {
        psmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
