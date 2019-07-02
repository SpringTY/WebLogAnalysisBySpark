package com.spring.Dao

import java.sql.{Connection, PreparedStatement}

import com.spring.Bean.VideoOrArticleTimesView
import com.spring.utils.MySQLUtils

import scala.collection.mutable.ListBuffer

object VideoOrArticleTimesDao {

  def insertToVideoTimes(list: ListBuffer[VideoOrArticleTimesView], cmsId: String): Unit = {
    val connection = MySQLUtils.getConnection()
    var statement: PreparedStatement = null

    // 设置自动提交失效，为批处理效率增加
    connection.setAutoCommit(false)
    try {
      var sql = "insert into #Times (day,cmsid,times)values (?,?,?)"
      sql = sql.replace("#", cmsId)
      statement = connection.prepareStatement(sql)

      for (elem <- list) {
        statement.setString(1, elem.day)
        statement.setLong(2, elem.cmsId)
        statement.setLong(3, elem.times)
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

  def insertToArticleTimes(list: ListBuffer[VideoOrArticleTimesView]): Unit = {
    val connection = MySQLUtils.getConnection()
    var statement: PreparedStatement = null

    // 设置自动提交失效，为批处理效率增加
    connection.setAutoCommit(false)
    try {
      val sql = "insert into articleTimes (day,cmsid,times)values (?,?,?)"
      statement = connection.prepareStatement(sql)

      for (elem <- list) {
        statement.setString(1, elem.day)
        statement.setLong(2, elem.cmsId)
        statement.setLong(3, elem.times)
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
