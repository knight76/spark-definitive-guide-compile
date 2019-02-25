package spark.example.tutorial.chapter06

import spark.example.common.SparkHelper
import org.apache.spark.sql.functions.{col, expr, not}
import org.apache.spark.sql.functions.{expr, pow}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}

object DataFrameApiMain extends SparkHelper {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

    val myManualSchema = StructType(Array(
      StructField("InvoiceNo", LongType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Quantity", LongType, true),
      StructField("InvoiceDate", TimestampType, true),
      StructField("UnitPrice", LongType, true),
      StructField("CustomerID", StringType, true),
      StructField("Country", StringType, true)
    ))

    val df = spark.read.format("csv")
      .option("header", true)
      .schema(myManualSchema)
      .load("origin-source/data/retail-data/by-day/2010-12-01.csv")


    df.printSchema
    df.createOrReplaceTempView("dfTable")

    // 6.2
    import org.apache.spark.sql.functions.lit
    df.select(lit(5), lit("five"), lit(5.0)).show(5)

    // 6.3
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where("InvoiceNo = 536365")
      .show(5, false)

    df.where("InvoiceNo <> 536365")
      .show(5, false)

    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
      .show(5, false)

    val DOTCodeFilter = col("StockCode") === "DOT"
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("StockCode", "unitPrice", "isExpensive").show(5)

    // 6.4
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerID"), fabricatedQuantity.alias("realQuantity")).show(2)

    df.selectExpr(
      "CustomerID",
      "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

    import org.apache.spark.sql.functions.{round, bround}
    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
    df.select(round(lit(2.5)), bround(lit("2.5"))).show(2)

    import org.apache.spark.sql.functions.{corr}
    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()
    df.describe().show()

    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    df.stat.approxQuantile("UnitPrice", quantileProbs, relError)

    df.stat.crosstab("StockCode", "Quantity").show()

    df.stat.freqItems(Seq("StockCode", "Quantity")).show()

    import org.apache.spark.sql.functions.monotonically_increasing_id
    df.select(monotonically_increasing_id()).show(5)

    // 6.5
    import org.apache.spark.sql.functions.{initcap}
    df.select(initcap(col("Description"))).show(2, false)

    import org.apache.spark.sql.functions.{lower, upper}
    df.select(col("Description"),
      lower(col("Description")),
      upper(lower(col("Description")))).show(2)

    import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}
    df.select(
      ltrim(lit("    HELLO    ")).as("ltrim"),
      rtrim(lit("    HELLO    ")).as("rtrim"),
      trim(lit("    HELLO    ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

    println("regex--")
    import org.apache.spark.sql.functions.regexp_replace
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")
    // |는 or를 의미한다.
    df.select(
      regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
      col("Description")).show(2)

    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description").show(3, false)

    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }):+expr("*") // 이 값을 추가할 수도 있다.

    df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
      .select("Description").show(3, false)

    // 6.6
    println("---datetime")
    df.printSchema()

    import org.apache.spark.sql.functions.{current_date, current_timestamp}
    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    dateDF.createOrReplaceTempView("dateTable")

    dateDF.printSchema()


    import org.apache.spark.sql.functions.{date_add, date_sub}
    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

    import org.apache.spark.sql.functions.{datediff, months_between, to_date}
    dateDF.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)

    dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end"))).show(1)

    import org.apache.spark.sql.functions.{to_date, lit}
    spark.range(5).withColumn("date", lit("2017-01-01"))
      .select(to_date(col("date"))).show(1)

    dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

    import org.apache.spark.sql.functions.to_date
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2"))
    cleanDateDF.createOrReplaceTempView("dateTable2")

    import org.apache.spark.sql.functions.to_timestamp
    cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

    cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
    cleanDateDF.filter(col("date2") > "'2017-12-12'").show()

    // 6.7
    import org.apache.spark.sql.functions.coalesce
    df.select(coalesce(col("Description"), col("CustomerId"))).show()

    df.na.drop()
    df.na.drop("any")
    df.na.drop("all")
    df.na.drop("all", Seq("StockCode", "InvoiceNo"))

    df.na.fill("All Null values become this string")
    df.na.fill(5, Seq("StockCode", "InvoiceNo"))
    val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
    df.na.fill(fillColValues)


    df.na.replace("Description", Map("" -> "UNKNOWN"))



    df.selectExpr("(Description, InvoiceNo) as complex", "*")
    df.selectExpr("struct(Description, InvoiceNo) as complex", "*")

    // 6.9
    import org.apache.spark.sql.functions.struct
    val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
    complexDF.createOrReplaceTempView("complexDF")

    complexDF.select("complex.Description")
    complexDF.select(col("complex").getField("Description"))

    complexDF.select("complex.*")

    import org.apache.spark.sql.functions.split
    df.select(split(col("Description"), " ")).show(2)

    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]").show(2)

    import org.apache.spark.sql.functions.size
    df.select(size(split(col("Description"), " "))).show(2) // shows 5 and 3

    import org.apache.spark.sql.functions.array_contains
    df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

    import org.apache.spark.sql.functions.{split, explode}

    df.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show(2)

    import org.apache.spark.sql.functions.map
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)").show(2)

    // 6. 10
    val jsonDF = spark.range(1).selectExpr("""
      '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

    import org.apache.spark.sql.functions.{get_json_object, json_tuple}
    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")).show(2)

    jsonDF.selectExpr(
      "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column").show(2)

    import org.apache.spark.sql.functions.to_json
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")))

    import org.apache.spark.sql.functions.from_json
    val parseSchema = new StructType(Array(
      new StructField("InvoiceNo", StringType,true),
      new StructField("Description", StringType,true)))
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)

    val udfExampleDF = spark.range(5).toDF("num")
    def power3(number:Double):Double = number * number * number
    power3(2.0)

    import org.apache.spark.sql.functions.udf
    val power3udf = udf(power3(_:Double):Double)

    udfExampleDF.select(power3udf(col("num"))).show()

    spark.udf.register("power3", power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)

    spark.range(5)

  }
}
