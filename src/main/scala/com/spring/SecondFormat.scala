package com.spring

import org.apache.spark.sql.{SaveMode, SparkSession}


object SecondFormat {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SecondFormat")
      .getOrCreate()
    import spark.implicits._
    val logDF = spark.sparkContext.textFile(args(0))
      .map(logItem => LogUtil.parse(logItem))
      .toDF()
    // 创建 view 来操作
    logDF.createOrReplaceTempView("userInfo")
    // 仅仅保留csmType为video和article的信息
    val newLogDF = spark.sql("select * from userInfo where csmType in ('video','article')")

    newLogDF.show(20)
    newLogDF.coalesce(1)
      .write
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .parquet(args(1))

    spark.stop()
  }
}
