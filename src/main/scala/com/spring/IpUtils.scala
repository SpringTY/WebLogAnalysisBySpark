package com.spring

import com.ggstar.util.ip.IpHelper

object IpUtils {
  def parse(ipAddress:String): String ={
    IpHelper.findRegionByIp(ipAddress)
  }

  def main(args: Array[String]): Unit = {
    println(parse("58.30.15.255"))
  }
}