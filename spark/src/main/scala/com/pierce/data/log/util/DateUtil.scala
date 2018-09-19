package com.pierce.data.log.util

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {

  // 原始时间格式
  val originalTime: FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  // 标准时间格式
  val standardTime: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parseTime(time: String): String ={
    standardTime.format(new Date(getTime(time)))
  }

  /**
    * 解析原始时间格式
    * [19/Sep/2018:09:44:15 +0800] => 时间戳
    * @param time
    * @return
    */
  def getTime(time: String): Long ={
    originalTime.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
  }

  def main(args: Array[String]): Unit = {
    val time = "[19/Sep/2018:09:44:15 +0800]"
    println(parseTime(time))
  }
}
