import project_build.{Dependencies, _}

name := "spark-definitive-guide-compile"

version := "1.0"

scalaVersion := Versions.scala

libraryDependencies ++= Dependencies.spark
libraryDependencies ++= Seq(
  Dependencies.sqliteJdbc
)



compileOrder := CompileOrder.JavaThenScala
