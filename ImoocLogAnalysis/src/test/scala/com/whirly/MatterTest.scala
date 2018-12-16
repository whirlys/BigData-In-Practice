package com.whirly

import scala.util.matching.Regex

object MatterTest {
  def main(args: Array[String]): Unit = {

    val pattern = "^\\d+".r
    val str = "Scala is Scalable and cool34"

    println(pattern findFirstIn str)
  }
}
