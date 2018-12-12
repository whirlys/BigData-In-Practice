package com.whirly

import java.sql.DriverManager

/**
  * JDBC 连接 ThriftServer 进行查询
  * 输出结果：
  * key:b , value:world
  * key:a , value:hello
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://master:10000", "whirly", "")
    val pstmt = conn.prepareStatement("select * from t")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println("key:" + rs.getString("key") +
        " , value:" + rs.getString("value"))
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}
