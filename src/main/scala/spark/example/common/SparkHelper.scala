package spark.example.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkHelper {
  lazy val sparkConf: SparkConf         = new SparkConf().setMaster("local[*]")
  implicit val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  lazy val sc: SparkContext             = spark.sparkContext
}
