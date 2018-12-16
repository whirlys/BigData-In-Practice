package com.whirly.entity

/**
  * 保存各个省市某天各自的 TopN 课程的统计结果，与 day_video_city_access_topn_stat 数据表对应
  */
case class DayCityVideoAccessStat(day:String, cmsId:Long, city:String,times:Long,timesRank:Int)
