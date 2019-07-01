package com.lr.lr

import org.apache.spark.sql.SparkSession

class DataFrameEx {

}
object DataFrameEx{
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
       .master("local[2]")
       .appName("App")
       .getOrCreate()
    val emps = sparkSession.read.text("/Users/spring/data/emp.txt")
    emps.printSchema()
    emps.show(2)
    emps.head(2).foreach(println)
    emps.select("name")
    sparkSession.stop()
  }
}