package com.spring.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtils {
  def getConnection(): Connection = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/userInfo?characterEncoding=UTF-8&useSSL=true", "root", "spring123")
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

  def close(connection: Connection, statement: PreparedStatement): Unit = {
    try {
      if (statement != null) {
        statement.close()
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
