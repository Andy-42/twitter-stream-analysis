name := "twitter-stream-analysis"

version := "0.1"

scalaVersion := "2.13.7"

val zioVersion = "1.0.12"
//val catsVersion = "2.6.1"
//val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % zioVersion,

  "dev.zio" %% "zio-config" % "2.0.0-M1",
  "dev.zio" %% "zio-config-magnolia" % "2.0.0-M1",
  "dev.zio" %% "zio-config-typesafe" % "2.0.0-M1",

  //  "dev.zio" %% "zio-streams" % zioVersion,

  //  "dev.zio" %% "zio-logging" % "0.5.14",

  //  "io.circe" %% "circe-core" % circeVersion,
  //  "io.circe" %% "circe-generic" % circeVersion,
  //  "io.circe" %% "circe-parser" % circeVersion,

  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)

// Compile / run / mainClass := Some("andy42.de.Mission1")