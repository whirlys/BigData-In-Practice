package com.whirly.util

object RegexUtil {
  val pattern = "^\\d+".r

  /**
    * 获取字符串开头的数字，找不到数字则返回 None
    */
  def findStartNumber(str: String): String = {
    val number = pattern.findFirstIn(str)
    if (number.isEmpty) "" else number get
  }

  def main(args: Array[String]): Unit = {
    println(findStartNumber("adsf"))
    println(findStartNumber("123adsf"))
    println(findStartNumber("456?.2/34adsf"))
    println(findStartNumber("789"))
  }
}
