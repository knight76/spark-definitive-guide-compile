package spark.example.tutorial.chapter03

import spark.example.common.SparkHelper

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

// 3.2
object FlightMain extends SparkHelper {
  def main(args: Array[String]): Unit = {

    // 3.2
    import spark.implicits._

    val flightsDF = spark.read.parquet("origin-source/data/flight-data/parquet/2010-summary.parquet")
    val flights = flightsDF.as[Flight]

    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)
      .foreach(println(_))

    flights
      .take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
      .foreach(println(_))

  }
}
