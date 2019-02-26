package spark.example.tutorial.chapter07

import spark.example.common.SparkHelper

object StructuredApiAggregationMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("origin-source/data/retail-data/all/*.csv")
      .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")

    println(df.count())

    import org.apache.spark.sql.functions.count
    df.select(count("StockCode")).show() // 541909

    import org.apache.spark.sql.functions.countDistinct
    df.select(countDistinct("StockCode")).show() // 4070

    import org.apache.spark.sql.functions.approx_count_distinct
    df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364

    import org.apache.spark.sql.functions.{first, last}
    df.select(first("StockCode"), last("StockCode")).show()

    import org.apache.spark.sql.functions.{min, max}
    df.select(min("Quantity"), max("Quantity")).show()

    import org.apache.spark.sql.functions.sum
    df.select(sum("Quantity")).show() // 5176450

    import org.apache.spark.sql.functions.sumDistinct
    df.select(sumDistinct("Quantity")).show() // 29310

    import org.apache.spark.sql.functions.{sum, count, avg, expr}

    df.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchases"),
      avg("Quantity").alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases"))
      .selectExpr(
        "total_purchases/total_transactions",
        "avg_purchases",
        "mean_purchases").show()


    import org.apache.spark.sql.functions.{var_pop, stddev_pop}
    import org.apache.spark.sql.functions.{var_samp, stddev_samp}
    df.select(
      var_pop("Quantity"),
      var_samp("Quantity"),
      stddev_pop("Quantity"),
      stddev_samp("Quantity"))
      .show()


    import org.apache.spark.sql.functions.{skewness, kurtosis}
    df.select(
      skewness("Quantity"),
      kurtosis("Quantity"))
      .show()


    import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
    df.select(
      corr("InvoiceNo", "Quantity"),
      covar_samp("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity"))
      .show()

    import org.apache.spark.sql.functions.{collect_set, collect_list}
    df.agg(collect_set("Country"), collect_list("Country")).show()

    df.groupBy("InvoiceNo", "CustomerId").count().show()

    import org.apache.spark.sql.functions.count

    df.groupBy("InvoiceNo")
      .agg(
        count("Quantity").alias("quan"),
        expr("count(Quantity)"))
      .show()


    df.groupBy("InvoiceNo")
      .agg("Quantity"->"avg", "Quantity"->"stddev_pop")
      .show()


    import org.apache.spark.sql.functions.{col, to_date}
    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.col
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    import org.apache.spark.sql.functions.max
    import org.apache.spark.sql.functions.{dense_rank, rank}

    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    import org.apache.spark.sql.functions.col

    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


    val dfNoNull = dfWithDate.drop()
    dfNoNull.createOrReplaceTempView("dfNoNull")

    val rolledUpDF = dfNoNull.rollup("Date", "Country")
      .agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rolledUpDF.show()

    rolledUpDF.where("Country IS NULL").show()
    rolledUpDF.where("Date IS NULL").show()

    dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
      .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

    import org.apache.spark.sql.functions.{grouping_id, sum, expr}

    dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)
      .show()

    val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

    pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()
  }
}
