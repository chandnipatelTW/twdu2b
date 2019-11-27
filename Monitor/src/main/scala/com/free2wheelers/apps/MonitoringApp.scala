package com.free2wheelers.apps
//
// import com.free2wheelers.apps.StationStatusTransformation._
// import org.apache.curator.framework.CuratorFrameworkFactory
// import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.SparkSession

object MonitoringApp {
  val log : Logger = LoggerFactory.getLogger(this.getClass);

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MonitoringApp")
      .getOrCreate()


    log.info("hello world!!")


    import spark.implicits._

    spark.stop()
  }
}
