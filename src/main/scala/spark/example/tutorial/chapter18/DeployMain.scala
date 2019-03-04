package spark.example.tutorial.chapter18

import spark.example.common.SparkHelper

object DeployMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("INFO")

  }
}
