package com.spring.Dao

import java.sql.PreparedStatement

import com.spring.Bean.{VideoOrArticleTimesBean, VideoRegionBean}
import com.spring.utils.MySQLUtils

import scala.collection.mutable.ListBuffer

object VideoRegionDao {
  def insertToVideoTimes(list: ListBuffer[VideoRegionBean]): Unit = {
    val connection = MySQLUtils.getConnection()
    var statement: PreparedStatement = null

    // 设置自动提交失效，为批处理效率增加
    connection.setAutoCommit(false)
    try {
      val sql = "insert into videoRegion (day,city,cmsid,times,times_rank)values (?,?,?,?,?)"
      statement = connection.prepareStatement(sql)
      for (elem <- list) {
        statement.setString(1, elem.day)
        statement.setString(2, elem.city)
        statement.setLong(3, elem.cmsId)
        statement.setLong(4, elem.times)
        statement.setLong(5, elem.times_rank)
        //加到batch中
        statement.addBatch()
      }
      // 执行批处理
      statement.executeBatch()
      // 手工提交
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.close(connection, statement)
    }
  }
}
