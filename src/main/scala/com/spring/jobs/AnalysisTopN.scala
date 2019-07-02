package com.spring.jobs

import com.spring.Bean.{VideoOrArticleTimesBean, VideoRegionBean, VideoTrafficBean}
import com.spring.Dao.{VideoOrArticleTimesDao, VideoRegionDao, VideoTrafficDao}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object AnalysisTopN {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AnalysisTopN")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val log = spark.read.parquet(args(0))

    //saveTopNVideoOrArticleByTimes(spark, log, "video")
    //saveTopNVideoOrArticleByTimes(spark, log, "article")
    //saveTopNVideoByRegion(spark, log, "20161110")
    saveTopNVideoByTraffic(spark, log, "20161110")
    spark.stop()
  }

  def saveTopNVideoByTraffic(spark: SparkSession, log: DataFrame, day: String): Unit ={
    val TopNVideoByTraffic = getTopNVideoByTraffic(spark, log, "20161110")
    TopNVideoByTraffic.foreachPartition(
      partition=>{
        val list = new ListBuffer[VideoTrafficBean]
        partition.foreach(elem=>{

          val day:String = elem.getAs("day")
          val cmsId:Long = elem.getAs("cmsId")
          val traffics:Long = elem.getAs("traffics")
          list.append(VideoTrafficBean(day,cmsId,traffics))
        })
        VideoTrafficDao.insertToVideoTraffic(list)
      }
    )
  }
  def getTopNVideoByTraffic(spark: SparkSession, log: DataFrame, day: String): Dataset[Row] = {
    import spark.implicits._
    log.filter($"day" === day && $"cmsType" === "video")
      .groupBy(log("day"), log("cmsId")).agg(
      sum("traffic").as("traffics")).orderBy($"traffics".desc)
  }

  def saveTopNVideoByRegion(spark: SparkSession, log: DataFrame, day: String): Unit = {
    val topVideoByTimes = getTopNVideoByRegion(spark, log, day)

    topVideoByTimes.foreachPartition(partition => {
      // 对每个分区操作
      var list = new ListBuffer[VideoRegionBean]
      partition.foreach(row => {
        // 对每行操作
        val day: String = row.getAs("day")
        val cmsId: Long = row.getAs("cmsId")
        val times: Long = row.getAs("times")
        val city: String = row.getAs("city")
        val times_rank: Long = row.getAs("times_rank").toString.toLong
        list.append(VideoRegionBean(day, city, cmsId, times, times_rank))
      })
      //添加到数据库
      VideoRegionDao.insertToVideoRegion(list)
    })
  }


  /**
    * 根据日期获取每个地区分类的TOP3
    *
    * @param spark
    * @param log
    * @param day
    * @return
    */
  def getTopNVideoByRegion(spark: SparkSession, log: DataFrame, day: String) = {
    import spark.implicits._

    val regionInfo = log.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))
      .select("day", "city", "cmsID", "times")


    //    regionInfo.createOrReplaceTempView("reginInfo")
    //    spark.sql("select * from " +
    //      "(select day,city,cmsid,times," +
    //      "row_number() over (partition by city order by times desc) " +
    //      "as times_rank from reginInfo) u " +
    //      "where u.times_rank<=3")
    val topNByRegion = regionInfo.select(
      regionInfo("day"),
      regionInfo("city"),
      regionInfo("cmsId"),
      regionInfo("times"),
      // row_number 不分组排序
      row_number().over(Window.partitionBy(regionInfo("city"))
        .orderBy(regionInfo("times").desc)).as("times_rank")
    ).filter($"times_rank" <= 3)

    topNByRegion
  }

  /**
    * 得到按次数需求的信息的DataFrame
    *
    * @param spark sparkSession
    * @param log   dataFrame
    * @param cmsId video 或者 article
    * @return
    */
  def getTopNByTimes(spark: SparkSession, log: DataFrame, cmsId: String): DataFrame = {
    log.createOrReplaceTempView("logInfo")
    var sql = "select day,cmsId,count(*) as times " +
      "from logInfo " +
      "where cmsType = '?' " +
      "group by day,cmsId " +
      "order by times desc"
    sql = sql.replace("?", cmsId)
    val VideoTimesDF = spark.sql(sql)
    VideoTimesDF
  }

  /**
    * 指定article或者video把结果持久化到数据库
    *
    * @param spark sparkSession
    * @param log   由 getTopNByTimes 生成的 dataFrame
    * @param cmsId video 或者 article
    */
  def saveTopNVideoOrArticleByTimes(spark: SparkSession, log: DataFrame, cmsId: String): Unit = {
    val topVideoByTimes = getTopNByTimes(spark, log, cmsId)

    topVideoByTimes.foreachPartition(partition => {
      // 对每个分区操作
      var list = new ListBuffer[VideoOrArticleTimesBean]
      partition.foreach(row => {
        // 对每行操作
        val day: String = row.getAs("day")
        val cmsId: Long = row.getAs("cmsId")
        val times: Long = row.getAs("times")
        list.append(VideoOrArticleTimesBean(day, cmsId, times))
      })
      //添加到数据库
      VideoOrArticleTimesDao.insertToVideoTimes(list, cmsId)
    })
  }

}
