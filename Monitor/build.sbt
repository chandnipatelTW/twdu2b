val sparkVersion = "2.3.0"

lazy val root = (project in file(".")).

settings(
  inThisBuild(List(
    organization := "com.free2wheelers",
    scalaVersion := "2.11.8",
    version := "0.0.1"
  )),

  name := "free2wheelers-monitor",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)
