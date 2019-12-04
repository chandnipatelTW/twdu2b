package com.free2wheelers.apps

import com.free2wheelers.apps.MonitoringApp.{Entry, Error}
import org.apache.spark.sql.SparkSession
import org.scalatest._

class PushMetricsToCloudWatchTest extends FeatureSpec with Matchers with GivenWhenThen {

  val entry = Entry(
    "8",
    "13",
    "true",
    "true",
    "1574915049",
    "0ff3bf339e08df67475b40f9ea275674",
    "Natoma St at New Montgomery St",
    "37.78655306258097",
    "-122.39960700273514"
  )

  val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
  import spark.implicits._

  feature("Push metrics to Cloudwatch") {
    scenario("Push number when duplicate ids found") {
      Given("Monitoring app found a duplicate station_id")
      val data = Seq(entry,entry).toDS()
      val duplicateError = Error("Duplicate station_id", data.as[MonitoringApp.Entry].collect())

      When("Create a request to cloudwatch")

      val metricDatum = MonitoringApp.toMetricDatum(duplicateError)

      Then("Returns a request to put metrics for duplicate ids")

      metricDatum.metricName() should be("Duplicate station_id")
      metricDatum.value() should be(2)
    }

    scenario("Push multiple errror types to cloudwatch") {
      Given("Monitoring app found multiple error types")
      val errorType1 = Error("ErrorType1", Seq(entry,entry).toDS().as[MonitoringApp.Entry].collect())
      val errorType2 = Error("ErrorType2", Seq(entry,entry, entry).toDS().as[MonitoringApp.Entry].collect())

      When("Calling cloudwatch")
      val response = MonitoringApp.createCloudWatchMetricRequest(Array(errorType1, errorType2))

      Then("Metric sent for each error type")
      response.metricData().get(0).value() should be(2)
      response.metricData().get(1).value() should be(3)

    }
  }
}
