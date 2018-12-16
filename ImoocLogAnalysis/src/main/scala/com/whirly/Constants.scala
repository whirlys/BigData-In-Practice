package com.whirly

object Constants {
  val protocol = "file:///"
  val rawPath = "F:/data/imooc-log/access.1w.log" // 原始数据
  val tempOut = "F:/data/imooc-log/tempClean" // 第一步清洗
  val cleanedIn = tempOut + "/part-*"  // 第二次清洗的输入路径
  val cleanedOut = "F:/data/imooc-log/cleaned" // 清洗完毕后
}
