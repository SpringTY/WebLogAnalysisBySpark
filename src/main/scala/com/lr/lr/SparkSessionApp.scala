package com.lr.lr

class SparkSessionApp {

}
object SparkSessionApp{
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .getOrCreate()

    val table = spark.read.text(args(0));
    table.show(2);
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    spark.stop()
  }
}