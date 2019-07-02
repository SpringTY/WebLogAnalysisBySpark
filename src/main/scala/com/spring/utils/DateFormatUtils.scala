package com.spring.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Date Format工具类
  * parse 实现把日志日期格式改成 yyyy-MM-dd HH:mm:ss格式
  * 注意SimpleDateFormat线程不安全
  */
object DateFormatUtils{
  val DDMMMYYY_HHMMSS_Format = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val TargetFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time : String): String ={
    TargetFormat.format(getDate(time))
  }
  def getDate(time:String): Long ={
    var timeDate = new Date()
    try{
      timeDate = DDMMMYYY_HHMMSS_Format.parse(time.substring(1,time.length-1))
      return timeDate.getTime
    }catch {
      case e: Exception => {
        println("ERROR")
        return 0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val s = DateFormatUtils.parse("[10/Nov/2016:00:01:02 +0800]")
    println(s)
  }

}
