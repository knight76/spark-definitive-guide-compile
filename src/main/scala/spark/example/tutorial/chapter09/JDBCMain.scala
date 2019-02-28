package spark.example.tutorial.chapter09

import spark.example.common.SparkHelper

object JDBCMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    val driver = "org.sqlite.JDBC"
    val path = "origin-source/data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:${path}"
    val tablename = "flight_info"

    // driver loading
    import java.sql.DriverManager
    Class.forName("org.sqlite.JDBC")
    val connection = DriverManager.getConnection(url)
    println(connection.isClosed)
    println(connection.close())


    val dbDataFrame = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", tablename)
      .option("driver", driver)
      .load()

    println("basic ---")
    println(dbDataFrame.select("DEST_COUNTRY_NAME").count())

    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain

    dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain

    val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info"""
    val newDbDataFrame = spark.read.format("jdbc")
      .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)
      .load()
    newDbDataFrame.explain()


    val partitionsDataFrame = spark.read.format("jdbc")
      .option("url", url).option("dbtable", tablename).option("driver", driver)
      .option("numPartitions", 10).load()

    partitionsDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)
    partitionsDataFrame.explain()

    //  predicates
    println("predicates--")
    val props = new java.util.Properties
    props.setProperty("driver", "org.sqlite.JDBC")
    val predicates = Array(
      "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
    println(spark.read.jdbc(url, tablename, predicates, props).count()) // 4
    println(spark.read.jdbc(url, tablename, predicates, props).rdd.getNumPartitions) // 2

    val predicates2 = Array(
      "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
      "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'")
    println(spark.read.jdbc(url, tablename, predicates2, props).count()) // 510
    println(spark.read.jdbc(url, tablename, predicates2, props).rdd.getNumPartitions) //2

    val colName = "count"
    val lowerBound = 0L
    val upperBound = 348113L
    val numPartitions = 10

    val count = spark.read
      .jdbc(url,tablename,colName,lowerBound,upperBound,numPartitions,props)
      .count()
    println(count) // 255

    // select expr
    spark.read.textFile("origin-source/data/flight-data/csv/2010-summary.csv")
      .selectExpr("split(value, ',') as rows").show(5)


    // write

    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    val myManualSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", LongType, true),
      new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
      new StructField("count", LongType, false) ))

    val csvFile = spark.read.format("csv")
      .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
      .load("/data/flight-data/csv/2010-summary.csv")

    csvFile.write.format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .save("/tmp/my-tsv-file.tsv")

    csvFile.write.format("json")
      .mode("overwrite")
      .save("/tmp/my-json-file.json")

    csvFile.write.format("parquet")
      .mode("overwrite")
      .save("/tmp/my-parquet-file.parquet")

    spark.read.format("orc")
      .load("/data/flight-data/orc/2010-summary.orc")
      .show(5)

    csvFile.write.format("orc")
      .mode("overwrite")
      .save("/tmp/my-json-file.orc")

    val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
    csvFile.write.mode("overwrite")
      .jdbc(newPath, tablename, props)

    csvFile.write.mode("append")
      .jdbc(newPath, tablename, props)

    csvFile
      .select("DEST_COUNTRY_NAME")
      .write
      .text("/tmp/simple-text-file.txt")

    csvFile
      .limit(10)
      .select("DEST_COUNTRY_NAME", "count")
      .write.partitionBy("count").text("/tmp/five-csv-files2.csv")

    csvFile.limit(10).write
      .mode("overwrite")
      .partitionBy("DEST_COUNTRY_NAME")
      .save("/tmp/partitioned-files.parquet")

    val numberBuckets = 10
    val columnToBucketBy = "count"

    csvFile.write.format("parquet")
      .mode("overwrite")
      .bucketBy(numberBuckets, columnToBucketBy)
      .saveAsTable("bucketedFiles")
  }
}
