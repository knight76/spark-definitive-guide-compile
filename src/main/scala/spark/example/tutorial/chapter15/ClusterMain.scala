package spark.example.tutorial.chapter15

import spark.example.common.SparkHelper

object ClusterMain extends SparkHelper {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 50)
    println(spark.sessionState.analyzer.resolver)

    import org.apache.spark.SparkContext
    val sc = SparkContext.getOrCreate()

    println(sc.sparkUser)
  }
}
