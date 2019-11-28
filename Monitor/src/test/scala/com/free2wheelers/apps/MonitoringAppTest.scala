package com.free2wheelers.apps

import com.free2wheelers.apps.MonitoringApp.{
  entriesWithDuplicateStationIds,
  entriesWithInvalidAvailableDocks,
  Entry,
  validate
}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalatest._

class MonitoringAppTest extends FeatureSpec with Matchers with GivenWhenThen {

val entryDuplicateStationIds1 = Entry(
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

val entryDuplicateStationIds2 = Entry(
  "9",
  "10",
  "true",
  "true",
  "1574915050",
  "0ff3bf339e08df67475b40f9ea275674",
  "Webster St at Clay St",
  "37.79080303242391",
  "-122.43259012699126"
)

val entryNegativeDocksAvailable = Entry(
  "8",
  "-4",
  "true",
  "true",
  "1574915049",
  "0ff3bf339e08df67475b40f9ea275674",
  "Natoma St at New Montgomery St",
  "37.78655306258097",
  "-122.39960700273514"
)

  feature("Validate station_id") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Returns false when station_id's are not unique") {
      Given("Data with duplicate station_id")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withDuplicateStationIds.csv")

      When("Validated")
      val result = entriesWithDuplicateStationIds(testDF, spark)

      Then("Return false")
      result.count should be(2)
      val Array(entry1, entry2) = result.head(2)

      entry1 should be(entryDuplicateStationIds1)
      entry2 should be(entryDuplicateStationIds2)
    }

    scenario("Return no rows when all rows have unique station_ids") {
      Given("Data with unique station_id only")
      val testDF = spark.read.format("csv")
        .option("header","true")
        .load("./src/test/resources/test.csv")

      When("Validated")
      val result = entriesWithDuplicateStationIds(testDF, spark)

      Then("Return no rows")
      result.count should be(0)
    }
  }

  feature("Validate docks_available") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._
    scenario("Return no rows when all rows have nonnegative docks_available") {
      Given("Data with docks_available greater than or equal to zero")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/test.csv")

      When("Validated")
      val result = entriesWithInvalidAvailableDocks(testDF, spark)

      Then("Return no rows")
      result.count should be(0)
    }

    scenario("Return row when docks_available is negative") {
      Given("Data with docks_available less than zero")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withNegativeDocks.csv")

      When("Validated")
      val result = entriesWithInvalidAvailableDocks(testDF, spark)

      Then("Return invalid rows")
      result.count should be(1)

      val entry = result.head
      entry should be(entryNegativeDocksAvailable)
    }
  }

  feature("Validate entries") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._
    scenario("Return invalid entries for invalid docks_available error") {
      Given("Invalid docks_available")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withNegativeDocks.csv")

      When("Validated")
      val errors = validate(testDF, spark)

      Then("Return invalid rows grouped by error")
      errors.length should be(1)

      val error = errors(0)

      error.message should be("Invalid docks_available")
      error.entries should be(Array(entryNegativeDocksAvailable))
    }

    scenario("Return invalid entries for duplicate station_id error") {
      Given("Duplicate station_id")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withDuplicateStationIds.csv")

      When("Validated")
      val errors = validate(testDF, spark)

      Then("Return invalid rows grouped by error")
      errors.length should be(1)

      val error = errors(0)
      error.message should be("Duplicate station_id")
      error.entries should be(Array(entryDuplicateStationIds1, entryDuplicateStationIds2))
    }
  }
}
