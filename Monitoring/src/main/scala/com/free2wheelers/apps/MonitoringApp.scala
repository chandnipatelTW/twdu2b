package com.free2wheelers.apps


import software.amazon.awssdk.services.cloudwatch.model.{MetricDatum, PutMetricDataRequest}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql._
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient

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

  def entriesWithInvalidAvailableBikes(df: DataFrame, spark: SparkSession): Dataset[Entry] = {
    import spark.implicits._

    df.filter($"bikes_available" < 0).as[Entry]
  }

  def entriesWithInvalidCoordinates(df: DataFrame, spark: SparkSession): Dataset[Entry] = {
    import spark.implicits._

    df.filter(isOutsideOfNYC($"latitude", $"longitude") && isOutsideOfSF($"latitude", $"longitude")).as[Entry]
  }

  def isOutsideOfNYC(latitude: Column, longitude: Column) : Column = {

    (latitude < 40 || latitude > 41 ) || (longitude > -73.5 || longitude < -74)
  }

  def isOutsideOfSF(latitude: Column, longitude: Column) : Column = {

    (latitude < 37 || latitude > 38 ) || (longitude > -121 || longitude < -123)
  }

  def validate(df: DataFrame, spark: SparkSession): Array[Error] = {

    val stationDuplicateError = Error(
      "Duplicate station_id",
      entriesWithDuplicateStationIds(df, spark).collect
    )
    val invalidDocksError = Error(
      "Invalid docks_available",
      entriesWithInvalidAvailableDocks(df, spark).collect
    )

    val invalidBikesAvailableError = Error(
      "Invalid bikes_available",
      entriesWithInvalidAvailableBikes(df, spark).collect
    )

    val outOfValidRegion = Error("Coordinates are out of valid region", entriesWithInvalidCoordinates(df, spark).collect())

    val response = Array(stationDuplicateError, invalidDocksError, outOfValidRegion, invalidBikesAvailableError).filter((error) => error.entries.length > 0)
    response
  }

  def sendMetricsToCloudWatch(monitoringResults: Array[Error]) = {
    val metricPutRequest = createCloudWatchMetricRequest(monitoringResults)
    metricPutRequest.metricData().add( MetricDatum.builder().metricName("health-check").value(monitoringResults.length.toDouble).build())
    log.info("Sending request to Cloudwatch")
    CloudWatchClient.create().putMetricData(metricPutRequest)
  }

  def createCloudWatchMetricRequest(monitoringResults: Array[Error]) = {
    val errorMetricDatum = monitoringResults.map(error => {
      toMetricDatum(error)
    })
    log.info("Creating PutMetricDataRequest to push metrics to Cloudwatch")
    PutMetricDataRequest.builder()
      .namespace("2Wheelers_DeliveryFile")
      .metricData(errorMetricDatum.toSeq:_*).build()
  }

  def toMetricDatum(error: Error): MetricDatum = {
      val metricDatum = MetricDatum.builder()
        .metricName(error.message)
        .value(error.entries.length.toDouble).build()
    log.info("Created MetricDatum for $metricDatum.metricName()")
    metricDatum
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MonitoringApp")
      .getOrCreate()


    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()


    val inputLocation = new String(
      zkClient.getData.watched.forPath("/free2wheelers/output/dataLocation"))

    log.info("Retrieving data from " + inputLocation)
    val errors = validate(spark.read.format("csv")
      .option("header", "true")
      .load(inputLocation), spark)

    log.info("Sending metrics for " + errors.length + " bad entries")
    sendMetricsToCloudWatch(errors)

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
