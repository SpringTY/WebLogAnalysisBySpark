package com.spring.Dao

import java.sql.PreparedStatement

import com.spring.Bean.VideoTrafficBean
import com.spring.utils.MySQLUtils

import scala.collection.mutable.ListBuffer

object VideoTrafficDao {
  def insertToVideoTraffic(list: ListBuffer[VideoTrafficBean]): Unit = {
    val connection = MySQLUtils.getConnection()
    var statement: PreparedStatement = null

    // 设置自动提交失效，为批处理效率增加
    connection.setAutoCommit(false)
    try {
      val sql = "insert into videoTraffic (day,cmsid,traffics) values (?,?,?)"
      statement = connection.prepareStatement(sql)
      for (elem <- list) {
        statement.setString(1, elem.day)
        statement.setLong(2, elem.cmsId)
        statement.setLong(3, elem.traffics)
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
