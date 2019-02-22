package spark.example.tutorial.chapter05

import spark.example.common.SparkHelper

object DataFrameMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructField, StructType, LongType, StringType}

    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, true)
    ))

    val myRows = Seq(Row("Hello", null, 1L), Row("World", "111", 2L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)

    myDf.show()

    myDf.select("some").show(2)

    myDf.select("some", "names").show(2)

    import org.apache.spark.sql.functions.{expr, col, column}
    import spark.implicits._
    myDf.select(
      myDf.col("some"),
      col("some"),
      column("some"),
      'some,
      $"some",
      expr("some"))
      .show(2)

    // myDf.select(expr("some"), "some").show(2) - error
    myDf.select(expr("some as s")).show(2)

    myDf.select(expr("some as s").alias("ssssss")).show(2)


    myDf.selectExpr(
      "*",
      "(some = col) as equals"
    ).show(2)

    myDf.selectExpr("avg(names)", "count(distinct(some))").show(2)

    import org.apache.spark.sql.functions.lit
    myDf.select(expr("*"), lit(1).as("One")).show(2)

    myDf.withColumn("numberOne", lit(1)).show(2)

    myDf.withColumn("some equals col", expr("some == col")).show(2)

    myDf.withColumn("some equals col", expr("some == col")).columns.foreach(println)

    myDf.withColumnRenamed("some", "Workpad").columns.foreach(println)

    myDf.withColumn("some-equals-col", expr("some == col")).show(2)

    val dfWithLongColName = myDf.withColumn("some-equals-col", expr("some == col"))
    dfWithLongColName.selectExpr(
      "`some-equals-col`",
      "`some-equals-col` as `new col`"
    ).show(2)

//    dfWithLongColName.selectExpr( -- error
//      "some-equals-col",
//      "some-equals-col as new col"
//    ).show(2)

    dfWithLongColName.select(col("some-equals-col")).columns.foreach(println)

    println
    dfWithLongColName.drop(col("some-equals-col")).columns.foreach(println)

    myDf.filter(col("names") > 1).show(2)
    myDf.where("names > 1").show(2)

    println("-")
    myDf.where(col("names") < 2).where(col("some") =!= "World").show(2)

    myDf.select("names", "col").distinct().count()

    myDf.select("names").distinct().count()

    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    myDf.sample(withReplacement, fraction, seed).count()

    val dataFrames = myDf.randomSplit(Array(0.25, 0.75), seed)
    println(dataFrames(0).count() > dataFrames(1).count())

    

  }


}
