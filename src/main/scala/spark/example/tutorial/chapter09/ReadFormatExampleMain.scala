package spark.example.tutorial.chapter09

import spark.example.common.SparkHelper

object ReadFormatExampleMain extends SparkHelper{
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))

    // csv
    spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("origin-source/data/flight-data/csv/2010-summary.csv")
      .show(5)

    val csvFile = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("origin-source/data/flight-data/csv/2010-summary.csv")

    csvFile.write.format("csv").mode("overwrite").option("sep", "\t")
      .save("/tmp/my-tsv-file.tsv")

    // json
    spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
      .load("origin-source/data/flight-data/json/2010-summary.json").show(5)

    csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")

    // paquet
    spark.read.format("parquet")
      .load("origin-source/data/flight-data/parquet/2010-summary.parquet").show(5)

    csvFile.write.format("parquet").mode("overwrite").save("/tmp/my-parquet-file.parquet")

    // orc
    spark.read.format("orc")
      .load("origin-source/data/flight-data/orc/2010-summary.orc").show(5)

    csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")

  }
}
