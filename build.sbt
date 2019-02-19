import project_build._

name := "spark-definitive-guide-compile"

version := "1.0"

scalaVersion := Versions.scala

libraryDependencies ++= Dependencies.spark

compileOrder := CompileOrder.JavaThenScala
