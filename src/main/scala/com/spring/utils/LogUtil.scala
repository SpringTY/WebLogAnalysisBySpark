package com.spring.utils

import com.spring.Bean.LogBean

object LogUtil {

  def parse(log: String): LogBean = {


    try {
      var splits = log.split("\t")
      val url = splits(0)
      val domain = "http://www.imooc.com/"
      var cmsInfo = "".split("")
      if (url.length > domain.length) {
        cmsInfo = url.substring(domain.length, url.length - 1).split("/")
      }
      var cmsType = ""
      var cmsId = 0l
      val ip = splits(3)
      val time = splits(1)
      val day = time.substring(0, 10).replace("-", "")
      val traffic = splits(2).toLong
      var city = IpUtils.parse(ip).replace(" ", "")
      if (cmsInfo.length > 1) {
        cmsType = cmsInfo(0)
        // 我也记录了一下发送请求的情况 注意 ? 用 [?] split
        cmsId = cmsInfo(1).split("[?]")(0).toLong
      }
      LogBean(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => {
        //println("Error parse: " + log)
        LogBean("", "", 0, 0, "", "", "", "")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val s = parse("http://www.imooc.com/code/645\t2016-11-10 06:13:23\t54\t218.81.140.3")

    print(s)
  }
}
