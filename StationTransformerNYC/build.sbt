val sparkVersion = "2.3.0"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.free2wheelers",
      scalaVersion := "2.11.8",
      version := "0.0.1"
    )),

    name := "free2wheelers-station-transformer-nyc",

    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.10",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.10",
    dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.10",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.apache.kafka" %% "kafka" % "1.1.1" % "test",
      "org.apache.curator" % "curator-test" % "2.10.0" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided" excludeAll(excludeJpountz),
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
    )
  )