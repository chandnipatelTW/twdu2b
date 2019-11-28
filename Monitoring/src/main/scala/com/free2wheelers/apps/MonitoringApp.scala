package com.free2wheelers.apps

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType

object MonitoringApp {
  val log : Logger = LoggerFactory.getLogger(this.getClass);

  def entriesWithDuplicateStationIds(df: DataFrame, spark: SparkSession): Dataset[Entry] = {
    import spark.implicits._

    val duplicatedStationId = df.groupBy($"station_id" as "duplicate_station_id")
                                .count()
                                .filter($"count" > 1)
                                .drop($"count")

    df.join(duplicatedStationId, $"station_id" === $"duplicate_station_id", "inner").as[Entry]
  }

  def entriesWithInvalidAvailableDocks(df: DataFrame, spark: SparkSession): Dataset[Entry] = {
    import spark.implicits._

    df.filter($"docks_available" < 0).as[Entry]
  }

  def validate(df: DataFrame, spark: SparkSession): Array[Error] = {
    import spark.implicits._

    val stationDuplicateError = Error(
      "Duplicate station_id",
      entriesWithDuplicateStationIds(df, spark).collect
    )
    val invalidDocksError = Error(
      "Invalid docks_available",
      entriesWithInvalidAvailableDocks(df, spark).collect
    )

    Array(stationDuplicateError, invalidDocksError).filter((error) => error.entries.length > 0)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MonitoringApp")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .load("./src/test/resources/test.csv")

    spark.stop()
  }

  case class Error(
    message: String,
    entries: Array[Entry]
  )

  case class Entry(
    bikes_available: String,
    docks_available: String,
    is_renting: String,
    is_returning: String,
    last_updated: String,
    station_id: String,
    name: String,
    latitude: String,
    longitude: String
  )
}
