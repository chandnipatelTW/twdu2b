#!/usr/bin/env bash

TRAINING_GROUP=twdu2b
WAITING_TIME_SECONDS=60

MESSAGE_TO_WRITE_IN_KAFKA=$(cat <<EOF
{
  "payload": {
    "network": {
      "company": [
        "JCDecaux"
      ],
      "href": "/v2/networks/test",
      "id": "test",
      "license": {
        "name": "Open Licence",
        "url": "https://developer.jcdecaux.com/#/opendata/licence"
      },
      "location": {
        "city": "Marseille",
        "country": "FR",
        "latitude": 43.296482,
        "longitude": 5.36978
      },
      "name": "TestStationData",
      "source": "https://developer.jcdecaux.com",
      "stations": [
        {
          "empty_slots": 16,
          "extra": {
            "address": "391 MICHELET - 391 BOULEVARD MICHELET",
            "banking": true,
            "bonus": false,
            "last_update": 1574754112000,
            "slots": 19,
            "status": "OPEN",
            "uid": 8149
          },
          "free_bikes": 3,
          "id": "686e48654a218c70daf950a4e893e5b0",
          "latitude": 43.25402727813068,
          "longitude": 5.401873594694653,
          "name": "8149-391 MICHELET",
          "timestamp": "2019-11-26T07:48:28.419000Z"
        }
      ]
    }
  }
}
EOF
)

function kafkaPublishMessageTest () {
    echo "============================="
    echo "1. Test: Kafka publish message"
    echo "============================="

	echo 'Message to write: '
	echo "$MESSAGE_TO_WRITE_IN_KAFKA"
		
	# Executing in a container env
	kafka_container=streamingdatapipeline_kafka_1
	docker exec "$kafka_container" sh -c "echo '$MESSAGE_TO_WRITE_IN_KAFKA' | /opt/kafka_2.11-0.10.0.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic station_data_test" 

	# TODO: Executing in a remote AWS Kafka instance
	#ssh kafka."$TRAINING_GROUP".training << EOF
  	#	kafka-console-producer --broker-list localhost:9092 --topic "station_data_test" <<< "$JSON_DATA_TEST"
	#EOF
		
    if [ $? -eq 1 ]
    then
        echo "Failure: Cant publish message in the topic in Kafka"
        exit 1
    else
        echo "Sucess: publish message to Kafka"
    fi

}

function validateOutputCSVFromHDFS () {
	  echo "==============================="
    echo "2. Validate output CSV from HDFS"
    echo "==============================="

	# Executing in a container env
	hadoop_container=streamingdatapipeline_hadoop_1
	result=$(docker exec "$hadoop_container" sh -c './usr/local/hadoop/bin/hadoop fs -cat /free2wheelers/stationMart/data/part-*.csv | grep TestStationData')

	# TODO: Executing in a remote AWS EMR instance
	#ssh emr-master."$TRAINING_GROUP".training << EOF
  	#	result=$(hadoop fs -cat /free2wheelers/stationMart/data/part-*.csv | grep TestStationData)
	#EOF

	if [ -z "$result" ]
    then
        echo "Failure: Previous message was not processed"
        exit 1
    else
        echo "Sucess: Previous message processed! yay!!!!!"
    fi

}

echo "============================="
echo "TwoWheelers E2E Test"
echo "============================="

kafkaPublishMessageTest
validateOutputCSVFromHDFS
