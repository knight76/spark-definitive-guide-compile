package spark.example.tutorial.chapter16

import spark.example.common.SparkHelper

object SparkApplicationMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("DefinitiveGuide")
      .set("some.conf", "to.some.value")

    println(conf.get("some.conf"))

    import scala.util.{Try, Success, Failure}
    val m = Try(conf.get("1ome.conf"))
    m match {
      case Success(v) =>
        println(v)
      case Failure(e) =>
        println("some error occurred")
        e.printStackTrace()
    }
  }
}
