package com.spring.jobs

import com.spring.Bean.VideoOrArticleTimesView
import com.spring.Dao.VideoOrArticleTimesDao
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object AnalysisTopN {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AnalysisTopN")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()
    val log = spark.read.parquet(args(0))

    saveTopNVideoOrArticleByTimes(spark, log, "video")
    saveTopNVideoOrArticleByTimes(spark, log, "article")
    spark.stop()
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
      var list = new ListBuffer[VideoOrArticleTimesView]
      partition.foreach(row => {
        // 对每行操作
        val day: String = row.getAs("day")
        val cmsId: Long = row.getAs("cmsId")
        val times: Long = row.getAs("times")
        list.append(VideoOrArticleTimesView(day, cmsId, times))
      })
      //添加到数据库
      VideoOrArticleTimesDao.insertToVideoTimes(list, cmsId)
    })
  }

}
