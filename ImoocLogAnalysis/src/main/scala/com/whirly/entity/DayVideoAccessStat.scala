package com.whirly.entity

/**
  * 每天课程访问次数实体类，与MySQL中的 day_video_access_topn_stat 表对应
  */
case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)