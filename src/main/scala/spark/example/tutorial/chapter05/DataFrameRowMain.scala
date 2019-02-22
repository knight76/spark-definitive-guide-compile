package spark.example.tutorial.chapter05

import spark.example.common.SparkHelper

object DataFrameRowMain extends SparkHelper {
  def main(args: Array[String]): Unit = {

    // 5.4.14 부터
    import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
    import org.apache.spark.sql.types.Metadata

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("json").schema(myManualSchema).load("origin-source/data/flight-data/json/2015-summary.json")

    import org.apache.spark.sql.Row

    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )

    val parallelizedRrows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRrows, schema)

    import spark.implicits._

    df.union(newDF)
      .where("count = 1")
      .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
      .show(2)

    df.sort("count").show(10)

    import org.apache.spark.sql.functions.col
    df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

    import org.apache.spark.sql.functions.{desc, asc, expr}
    df.orderBy(expr("count desc")).show(2)
    df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

    spark.read.format("json").load("origin-source/data/flight-data/json/*-summary.json")
      .sortWithinPartitions("count")

    df.limit(5).show()

    df.rdd.getNumPartitions
    df.repartition(col("DEST_COUNTRY_NAME"))
    df.repartition(5, col("DEST_COUNTRY_NAME"))
    df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

    val collectDF = df.limit(10)
    collectDF.take(5)
    collectDF.show()
    collectDF.show(5, false)
    collectDF.collect
  }
}
