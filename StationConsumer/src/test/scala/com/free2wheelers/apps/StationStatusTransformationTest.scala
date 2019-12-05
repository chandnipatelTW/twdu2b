package com.free2wheelers.apps

import java.sql.Timestamp

import com.free2wheelers.apps.StationStatusTransformation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalatest._

class StationStatusTransformationTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Transform nyc station data frame") {

      val testStationData =
        """{
          "station_id":"83",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":-73.97632328
          }"""

      val schema = ScalaReflection.schemaFor[StationStatus].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(nycStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("timestamp")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(19)
      row1.get(1) should be(41)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(new Timestamp(1536242527L * 1000))
      row1.get(5) should be("83")
      row1.get(6) should be("Atlantic Ave & Fort Greene Pl")
      row1.get(7) should be(40.68382604)
      row1.get(8) should be(-73.97632328)
    }

    scenario("Transform sf station data frame") {

      val testStatusData =
        """{
        "metadata": {
          "producer_id": "producer_station-san_francisco",
          "size": 1323,
          "message_id": "1234-3224-2444242-fm2kf23",
          "ingestion_time": 1524493544235
        },
        "payload": {
          "network": {
            "company": [
              "Motivate International, Inc."
            ],
            "gbfs_href": "https://gbfs.fordgobike.com/gbfs/gbfs.json",
            "href": "/v2/networks/ford-gobike",
            "id": "ford-gobike",
            "location": {
              "city": "San Francisco Bay Area, CA",
              "country": "US",
              "latitude": 37.7141454,
              "longitude": -122.25
            },
            "name": "Ford GoBike",
            "stations": [
              {
                "empty_slots": 26,
                "extra": {
                  "address": null,
                  "last_updated": 1535550884,
                  "renting": null,
                  "returning": 1,
                  "uid": "56"
                },
                "free_bikes": 1,
                "id": "0d1cc38593e42fd252223058f5e2a1e3",
                "latitude": 37.77341396997343,
                "longitude": -122.42731690406801,
                "name": "Koshland Park",
                "timestamp": "2018-08-29T13:58:57.031000Z"
              },
              {
                "empty_slots": 9,
                "extra": {
                  "address": null,
                  "last_updated": 1535550804,
                  "renting": 1,
                  "returning": 1,
                  "uid": "152"
                },
                "free_bikes": 10,
                "id": "744a78dbf1295803e62b64fd7579ddef",
                "latitude": 37.83563220458518,
                "longitude": -122.28105068206787,
                "name": "47th St at San Pablo Ave",
                "timestamp": "2018-08-29T13:58:57.184000Z"
              }
            ]
          }
        }
      }"""

      val schema = ScalaReflection.schemaFor[StationStatus].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStatusData).toDF("raw_payload")

      When("Transformations are applied")
      val resultDF1 = testDF1.transform(sfStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("timestamp")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(10)
      row1.get(1) should be(9)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(5) should be("744a78dbf1295803e62b64fd7579ddef")
      row1.get(6) should be("47th St at San Pablo Ave")
      row1.get(7) should be(37.83563220458518)
      row1.get(8) should be(-122.28105068206787)
    }
  }
}
