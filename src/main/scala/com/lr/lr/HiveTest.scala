package com.lr.lr
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

class HiveTest {

}
object HiveTest{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc);

    hiveContext.table("emp").show()
    sc.stop()
  }
}
