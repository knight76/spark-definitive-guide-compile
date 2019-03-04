package spark.example.tutorial.chapterr14

import org.apache.spark.util.AccumulatorV2
import spark.example.common.SparkHelper

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
  private var num:BigInt = 0

  def reset(): Unit = {
    this.num = 0
  }

  def add(intValue: BigInt): Unit = {
    if (intValue % 2 == 0) {
      this.num += intValue
    }
  }

  def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = {
    this.num += other.value
  }

  def value():BigInt = {
    this.num
  }
  def copy(): AccumulatorV2[BigInt,BigInt] = {
    new EvenAccumulator
  }

  def isZero():Boolean = {
    this.num == 0
  }
}

object DistributedVariablesMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)

    val supplementalData = Map(
      "Spark" -> 1000, "Definitive" -> 200,
      "Big" -> -300, "Simple" -> 100)

    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
    println(suppBroadcast.value)

    words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()
      .foreach(println)

    import spark.implicits._
    val flights = spark.read
      .parquet("origin-source/data/flight-data/parquet/2010-summary.parquet")
      .as[Flight]

    import org.apache.spark.util.LongAccumulator
    val accUnnamed = new LongAccumulator
    val acc = spark.sparkContext.register(accUnnamed)

    val accChina = new LongAccumulator
    val accChina2 = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(accChina, "China")

    def accChinaFunc(flight_row: Flight) = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        accChina.add(flight_row.count.toLong)
      }
      if (origin == "China") {
        accChina.add(flight_row.count.toLong)
      }
    }

    flights.foreach(flight_row => accChinaFunc(flight_row))

    println(accChina.value) //953

    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.util.AccumulatorV2

    val arr = ArrayBuffer[BigInt]()

    val evenAcc = new EvenAccumulator
    val newAcc = sc.register(evenAcc, "evenAcc")

    println(evenAcc.value) // 0
    flights.foreach(flight_row => evenAcc.add(flight_row.count))
    println(evenAcc.value) // 31390

  }
}
