package com.spring.jobs

import com.spring.utils.DateFormatUtils
import org.apache.spark.sql.SparkSession

object FirstFormat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FirstFormat")
      .getOrCreate()
    val log = spark.sparkContext.textFile(args(0))

    val logFormat = log.map(item => {
      val splits = item.split("\\s+")
      val ipAddress = splits(0)
      val dateTime = DateFormatUtils.parse(splits(3) + " " + splits(4))
      val traffic = splits(9)
      val url = splits(11).replace("\"", "")
      url + "\t" + dateTime + "\t" + traffic + "\t" + ipAddress
    })
    logFormat.saveAsTextFile(args(1))
    //    print(log)
    println("ok")
    spark.stop()
  }
}
