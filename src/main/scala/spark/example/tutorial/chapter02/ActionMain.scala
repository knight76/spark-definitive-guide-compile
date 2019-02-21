package spark.example.tutorial.chapter02

import org.apache.spark.sql.DataFrame
import spark.example.common.SparkHelper

object ActionMain extends SparkHelper {
  def main(args: Array[String]): Unit = {

    // 2.9
    val flightData2015: DataFrame = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("origin-source/data/flight-data/csv/2015-summary.csv")

    flightData2015
      .sort("count")
      .explain

    spark.conf.set("spark.sql.shuffle.partitions", "5")
    flightData2015
      .sort("count")
      .show(2)

    // 2.10
    flightData2015.createOrReplaceTempView("flight_data_2015")
    val sqlWay = spark.sql("""
        |SELECT DEST_COUNTRY_NAME, count(1)
        |FROM flight_data_2015
        |GROUP BY DEST_COUNTRY_NAME
      """.stripMargin)

    val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count

    sqlWay.explain
    dataFrameWay.explain

    spark.sql("SELECT max(count) FROM flight_data_2015").take(1)

    import org.apache.spark.sql.functions.max
    flightData2015.select(max("count")).take(1)

    val maxSql = spark.sql(
      """
        |SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        |FROM flight_data_2015
        |GROUP BY DEST_COUNTRY_NAME
        |ORDER BY sum(count) DESC
        |LIMIT 5
      """.stripMargin)

    maxSql.show()

    import org.apache.spark.sql.functions.desc
    flightData2015.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

    spark.stop()
  }
}
