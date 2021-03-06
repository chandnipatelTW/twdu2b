package com.free2wheelers.apps

import java.sql.Timestamp
import java.time.Instant
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object StationStatusTransformation {

  val log : Logger = LoggerFactory.getLogger(this.getClass)

  val sfToStationStatus: String => Seq[StationStatus] = raw_payload => {
      val json: Option[Any] = JSON.parseFull(raw_payload)
      try {
        val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
        extractSFStationStatus(payload)
      } catch {
        case e: Exception => log.warn( "- StationConsumer:"  + e.getMessage, raw_payload)
          Seq()
      }
  }

  private def extractSFStationStatus(payload: Any) = {

    val network: Any = payload.asInstanceOf[Map[String, Any]]("network")

    val stations: Any = network.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .filter(r => r("extra").asInstanceOf[Map[String, Any]]("renting") != null)
      .map(x => {
        StationStatus(
          x("free_bikes").asInstanceOf[Double].toInt,
          x("empty_slots").asInstanceOf[Double].toInt,
          x("extra").asInstanceOf[Map[String, Any]]("renting").asInstanceOf[Double] == 1,
          x("extra").asInstanceOf[Map[String, Any]]("returning").asInstanceOf[Double] == 1,
          Timestamp.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(x("timestamp").asInstanceOf[String]))),
          x("id").asInstanceOf[String],
          x("name").asInstanceOf[String],
          x("latitude").asInstanceOf[Double],
          x("longitude").asInstanceOf[Double]
        )
      })
  }

  def sfStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(sfToStationStatus)

    import spark.implicits._

    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }

  def nycStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    jsonDF.select(from_json($"raw_payload", ScalaReflection.schemaFor[StationStatus].dataType) as "status")
      .select($"status.*")
  }
}
