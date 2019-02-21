package spark.example.tutorial.chapter03

import spark.example.common.SparkHelper

// 3.3
object StructMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    //sparkSession.conf.set("spark.sql.shuffle.partitions", 5)

    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferScheme", "true")
      .load("origin-source/data/retail-data/by-day/*.csv")
    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema

    println(staticSchema)

    import org.apache.spark.sql.functions.{window, col}
    staticDataFrame.selectExpr("CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate"    )
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load("origion-source/data/retail-data/by-day/*.csv")

    println("is streaming : " + streamingDataFrame.isStreaming)

    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")

    purchaseByCustomerPerHour.writeStream
      .format("memory")
      .queryName("customer_purchases")
      .outputMode("complete")
      .start()

    spark.sql(
      """
        SELECT *
        FROM customer_purchases
        ORDER BY `sum(total_cost)` DESC
      """.stripMargin)
      .show(5)

    // 3.4
    staticDataFrame.printSchema


    import org.apache.spark.sql.functions.date_format
    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
      .coalesce(5)

    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame
      .where("InvoiceData >= '2011-07-01")

    trainDataFrame.count()
    testDataFrame.count()

    import org.apache.spark.ml.feature.StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    import org.apache.spark.ml.feature.OneHotEncoder
    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    import org.apache.spark.ml.feature.VectorAssembler

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")

    import org.apache.spark.ml.Pipeline

    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))

    val fittedPipeline = transformationPipeline.fit(trainDataFrame)

    val transformedTraining = fittedPipeline.transform(trainDataFrame)

    transformedTraining.cache()

    import org.apache.spark.ml.clustering.KMeans
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)


    val kmModel = kmeans.fit(transformedTraining)
    kmModel.computeCost(transformedTraining)

    val transformedTest = fittedPipeline.transform(testDataFrame)
    kmModel.computeCost(transformedTest)


  }
}
