package com.whirly

import com.ggstar.util.ip.IpHelper

object IpHelperTest {
  def main(args: Array[String]): Unit = {
    println(IpHelper.findRegionByIp("10.100.0.1"))
    println(IpHelper.findRegionByIp("183.240.130.23"))
    println(IpHelper.findRegionByIp("119.130.229.90"))
    println(IpHelper.findRegionByIp("113.45.39.241"))
    println(IpHelper.findRegionByIp("39.188.192.245"))
    println(IpHelper.findRegionByIp("218.26.76.143"))

    /**
      * 全球
      * 广东省
      * 广东省
      * 北京市
      * 浙江省
      * 山西省
      */
  }
}
