package com.whirly.util

import com.ggstar.util.ip.IpHelper
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换(输入==>输出)工具类
  */
object AccessConvertUtil {
  //定义的输出的字段
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )


  /**
    * 根据输入的每一行信息转换成输出的样式
    *
    * @param log 输入的每一行记录信息: 1970-01-01 08:00:00	http://www.imooc.com/code/547	54	119.130.229.90
    */
  def parseLog(log: String) = {

    try {
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      // http://www.imooc.com/code/547   ===>  code/547  547
      var cmsType = ""
      var cmsId = 0l

      val domain = "http://www.imooc.com/"
      val domainIndex = url.indexOf(domain)
      if (domainIndex >= 0) {
        val cms = url.substring(domainIndex + domain.length)
        val cmsTypeId = cms.split("/")

        if (cmsTypeId.length > 1) {
          cmsType = cmsTypeId(0)
          if ("video".equals(cmsType) || "code".equals(cmsType) || "learn".equals(cmsType)) {
            try {
              cmsId = cmsTypeId(1).toLong
            } catch {
              case e: Exception => {}
            }
          } else if ("article".equals(cmsType)) {
            val number = RegexUtil.findStartNumber(cmsTypeId(1))
            if (StringUtils.isNotEmpty(number)) {
              cmsId = number.toLong
            }
          }
        }
      } /*else {
        val domain = "http://coding.imooc.com/"
        val domainIndex = url.indexOf(domain)
        if (domainIndex >= 0) {
          val cms = url.substring(domainIndex + domain.length)
          val cmsTypeId = cms.split("/")
          if (cmsTypeId.length > 1) {
            cmsType = cmsTypeId(0)
            if ("lesson".equals(cmsType)) {
              cmsId = cmsTypeId(1).toLong
            }
          }
        }
      }*/

      val city = IpHelper.findRegionByIp(ip)
      //IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      //这个row里面的字段要和struct中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => {
        println("------------log----------------------")
        println(log)
        Row(0)
      }
    }
  }
}
