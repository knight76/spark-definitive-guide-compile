package spark.example.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkHelper {
  lazy val sparkConf: SparkConf  = new SparkConf().setMaster("local[1]")
  implicit val sparkSession      = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  lazy val sc: SparkContext      = sparkSession.sparkContext
}
