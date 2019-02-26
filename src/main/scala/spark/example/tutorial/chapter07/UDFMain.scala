package spark.example.tutorial.chapter07

import spark.example.common.SparkHelper
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


class BoolAnd extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType = StructType(StructField("value", BooleanType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("result", BooleanType) :: Nil)

  def dataType: DataType = BooleanType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }

  def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}

object UDFMain extends SparkHelper {

  def main(args: Array[String]): Unit = {

    val booland = new BoolAnd
    spark.udf.register("booland", booland)

    spark.range(1)
      .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
      .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
      .select(booland(col("t")), expr("booland(f)"))
      .show()
  }
}
