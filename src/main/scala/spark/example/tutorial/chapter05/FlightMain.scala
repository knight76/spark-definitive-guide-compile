package spark.example.tutorial.chapter05

import spark.example.common.SparkHelper

object FlightMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    // 스키마(StructType) 없이 데이터 프레임 생성
    val flightsDF = spark.read.format("json").load("origin-source/data/flight-data/json/2015-summary.json")
    flightsDF.printSchema()

    // 스키마 기반(StructType)으로 데이터 프레임 생성
    import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
    import org.apache.spark.sql.types.Metadata

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val flightDF = spark.read.format("json").schema(myManualSchema).load("origin-source/data/flight-data/json/2015-summary.json")
    flightDF.printSchema()

    println("-- columns ")
    flightDF.columns.foreach(println)

    println("-- first")
    flightDF.first

    // 5.4.1 데이터 프레임 생성

    flightDF.createOrReplaceTempView("dfTable")


  }
}
