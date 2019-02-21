import spark.example.common.SparkHelper

object RDDMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val rdd = sc.parallelize(Seq(1,2,3)).toDF()
    rdd.printSchema()
    println(rdd)
  }
}
