package project_build
import sbt._

object Dependencies {

  val spark = Seq(
    "org.apache.spark" %% "spark-yarn"      % Versions.spark,
    "org.apache.spark" %% "spark-core"      % Versions.spark,
    "org.apache.spark" %% "spark-sql"       % Versions.spark,
    "org.apache.spark" %% "spark-mllib"     % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,
    "org.apache.spark" %% "spark-hive"      % Versions.spark,
    "org.apache.spark" %% "spark-avro"      % Versions.spark
  )

  val sqliteJdbc = "org.xerial" % "sqlite-jdbc" % "3.25.2"

}
