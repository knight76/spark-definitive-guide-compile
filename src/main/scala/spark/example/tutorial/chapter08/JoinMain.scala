package spark.example.tutorial.chapter08

import spark.example.common.SparkHelper

object JoinMain extends SparkHelper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")


    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpression).show()

    val wrongJoinExpression = person.col("name") === graduateProgram.col("school")
    person.join(graduateProgram, wrongJoinExpression).show() // ro result

    println("inner join")
    person.join(graduateProgram, joinExpression, "inner").show()

    println("outer join")
    person.join(graduateProgram, joinExpression, "outer").show()

    println("left outer join")
    person.join(graduateProgram, joinExpression, "left_outer").show()

    println("right outer join")
    person.join(graduateProgram, joinExpression, "right_outer").show()

    println("left semi join")
    graduateProgram.join(person, joinExpression, "left_semi").show()


    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())

    gradProgram2.createOrReplaceTempView("gradProgram2")
    gradProgram2.join(person, joinExpression, "left_semi").show()


    println("left anti join")
    graduateProgram.join(person, joinExpression, "left_anti").show()

    println("cross join")
    graduateProgram.join(person, joinExpression, "cross").show()

    person.crossJoin(graduateProgram).show()

    // join + expr
    import org.apache.spark.sql.functions.expr

    person.withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
    val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
    person.join(gradProgramDupe, joinExpr).show()

    // Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'graduate_program' is ambiguous, could be: graduate_program, graduate_program.;
//    person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
    person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
      .select("graduate_program").show()

    val joinExpr2 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr2).drop(graduateProgram.col("id")).show()

    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    val joinExpr3 = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr3).show()


    // explain
    val joinExpr4 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr4).explain() // spark 2.4 -> translated broadcast join

    import org.apache.spark.sql.functions.broadcast
    val joinExpr5 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(broadcast(graduateProgram), joinExpr5).explain()

  }
}
