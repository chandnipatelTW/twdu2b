package com.free2wheelers.apps

import com.free2wheelers.apps.MonitoringApp.{
  entriesWithDuplicateStationIds,
  entriesWithInvalidAvailableDocks,
  entriesWithInvalidAvailableBikes,
  entriesWithInvalidCoordinates,
  Entry,
  validate
}

import org.apache.spark.sql.SparkSession
import org.scalatest._

class MonitoringAppTest extends FeatureSpec with Matchers with GivenWhenThen {

  val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

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

  val entryNegativeBikesAvailable = Entry(
    "-9",
    "10",
    "true",
    "true",
    "1574915050",
    "chl0ef339e08df67475b40f9ea275674",
    "Webster St at Clay St",
    "37.79080303242391",
    "-122.43259012699126"
  )

  val entryWithLatitudeOutsideNYC = Entry(
    "8",
    "4",
    "true",
    "true",
    "1574915049",
    "0ff3bf339e08df67475b40f9ea275674",
    "Habana Cubaish",
    "23.104049",
    "-73.985533"
  )

  val entryWithLongitudeOutsideNYC = Entry(
    "9",
    "10",
    "true",
    "true",
    "1574915050",
    "chl0ef339e08df67475b40f9ea275674",
    "Bulo Somaliaish",
    "40.744511",
    "45.508442"
  )

  val entryWithLongitudeOutsideSF = Entry(
    "8",
    "4",
    "true",
    "true",
    "1574915049",
    "0ff3bf339e08df67475b40f9ea275674",
    "ole Monterrey",
    "37.611448",
    "-120.903720"
  )

  val entryWithLatitudeOutsideSF = Entry(
    "8",
    "4",
    "true",
    "true",
    "1574915049",
    "0ff3bf339e08df67475b40f9ea275674",
    "other Monterrey",
    "36.611448",
    "-122.903720"
  )

  feature("Validate input file") {
    scenario("Returns empty set for valid input") {
      Given("Valid input")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/test.csv")

      When("Executing validate")
      val rowsInError = validate(testDF, spark)

      Then("No rows should be returned")
      rowsInError should be(empty)
    }
  }

  feature("Validate station_id is unique") {
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

  feature("Validate docks_available are non-negative") {
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

  feature("Validate bikes are non-negative") {
    scenario("Return no rows when all rows have nonnegative bikes") {
      Given("Data with bikes available are greater than or equal to zero")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/test.csv")

      When("Validated")
      val result = entriesWithInvalidAvailableBikes(testDF, spark)

      Then("Return no rows")
      result.count should be(0)
    }
    scenario("Return row when bikes_available is negative") {
      Given("Data with bikes_available less than zero")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withNegativeBikes.csv")

      When("Validated ")
      val result = entriesWithInvalidAvailableBikes(testDF, spark)

      Then("Return invalid rows")
      result.count should be(1)

      val entry = result.head
      entry should be(entryNegativeBikesAvailable)
    }
  }

  feature("Validate latitude and longitude is inside of Range of Serviced Cities") {
    scenario("Return no rows when all rows are in NYC") {
      Given("Data with coordinates of NYC")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withCoordinatesInNYC.csv")

      When("Validated")
      val result = entriesWithInvalidCoordinates(testDF, spark)

      Then("Return no rows")
      result.count should be(0)
    }

    scenario("Return rows outside of NYC") {
      Given("Data with coordinates outside NYC")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withCoordinatesOutsideNYC.csv")

      When("Validated")
      val result = entriesWithInvalidCoordinates(testDF, spark)

      Then("Return row with coordinates outside NYC")
      result.count should be(2)

      val Array(entry1, entry2) = result.head(2)
      entry1 should be(entryWithLatitudeOutsideNYC)
      entry2 should be(entryWithLongitudeOutsideNYC)
    }

    scenario("Return no rows when all rows are in SF") {
      Given("Data with coordinates of SF")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withCoordinatesInSF.csv")

      When("Validated")
      val result = entriesWithInvalidCoordinates(testDF, spark)

      Then("Return no rows")
      result.count should be(0)
    }

    scenario("Return rows outside of SF") {
      Given("Data with coordinates outside SF")
      val testDF = spark.read.format("csv")
        .option("header", "true")
        .load("./src/test/resources/withCoordinatesOutsideSF.csv")

      When("Validated")
      val result = entriesWithInvalidCoordinates(testDF, spark)

      Then("Return row with coordinates outside SF")
      result.count should be(2)

      val Array(entry1, entry2) = result.head(2)
      entry1 should be(entryWithLongitudeOutsideSF)
      entry2 should be(entryWithLatitudeOutsideSF)
    }
  }

  feature("Validate entries") {
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
