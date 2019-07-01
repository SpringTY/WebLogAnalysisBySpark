package com.lr.lr
import org.apache.spark.sql.SparkSession
class SparkSessionEx {

}
object SparkSessionEx{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SqlSession")
      .master("local[2]").getOrCreate()
    val emp = spark.read.format("json").load(args(0))
    emp.printSchema()
    emp.show()
    spark.stop()
  }
}
