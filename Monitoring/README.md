# Monitor!

To run locally on the hadoop spark container, run


`sbt package`


then to submit the spark job


`docker exec -it streamingdatapipeline_spark-hadoop spark-submit --class com.free2wheelers.apps.MonitoringApp --master local /jobs/free2wheelers-monitoring_2.11-0.0.1.jar`