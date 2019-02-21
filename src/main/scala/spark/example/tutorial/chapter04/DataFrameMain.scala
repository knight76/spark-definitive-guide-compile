package spark.example.tutorial.chapter04

import spark.example.common.SparkHelper

object DataFrameMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    val df = spark.range(500).toDF("number")
    df.select(df.col("number") + 10)
  }

}
