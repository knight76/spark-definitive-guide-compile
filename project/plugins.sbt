logLevel := Level.Warn

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Maven central" at "http://repo1.maven.org/maven2/"
)

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.3")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.8")
addSbtPlugin("com.geirsson" % "sbt-scalafmt"  % "1.5.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
