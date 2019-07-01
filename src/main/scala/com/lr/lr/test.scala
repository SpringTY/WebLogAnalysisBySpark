package com.lr.lr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

class test {

}
object test{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.setAppName("SQLTest")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.format("json").load("/Users/spring/data/test.json")

    people.show(10)
    sc.stop()
    people.printSchema()
  }
}
