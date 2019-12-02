#!/usr/bin/env bash

TRAINING_GROUP=twdu2b

NOW_TIMESTAMP=$(date +"%FT%T.%6NZ")

VALID_MESSAGE_TO_WRITE_IN_KAFKA=$(cat <<EOF
{
  "payload": {
    "network": {
      "company": [
        "TwoWheelers"
      ],
      "href": "/v2/networks/test",
      "id": "test",
      "license": {
        "name": "Open Licence",
        "url": "https://developer.jcdecaux.com/#/opendata/licence"
      },
      "location": {
        "city": "Bangalore",
        "country": "IN",
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
          "id": "TestID",
          "latitude": 43.25402727813068,
          "longitude": 5.401873594694653,
          "name": "TestStationData",
          "timestamp": "$NOW_TIMESTAMP"
        }
      ]
    }
  }
}
EOF
)

function shouldReturnValidStatusWhenKafkaProducerWriteValidMessageInTopic () {
    echo "============================="
    echo "1. Test: Kafka publish message"
    echo "============================="

    echo 'Message to write: '
    echo "$VALID_MESSAGE_TO_WRITE_IN_KAFKA"

ssh kafka."$TRAINING_GROUP".training << 'kafkaTestCommand'
kafka-console-producer --broker-list localhost:9092 --topic station_data_test <<< "$VALID_MESSAGE_TO_WRITE_IN_KAFKA"
logout
kafkaTestCommand
		
    if [ $? -eq 1 ]
    then
        echo "Failure: Cant publish message in the topic in Kafka"
        exit 1
    else
        echo "Sucess: publish message to Kafka"
    fi

}

function shouldReturnNotEmptyWhenRetrieveTheHDFSFile () {
	  echo "==============================="
    echo "2. Validate output CSV from HDFS"
    echo "==============================="

ssh emr-master."$TRAINING_GROUP".training << 'HDFSTestCommand'
result=$(hadoop fs -cat /free2wheelers/stationMart/data/part-*.csv | grep TestStationData)
HDFSTestCommand

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

shouldReturnValidStatusWhenKafkaProducerWriteValidMessageInTopic
shouldReturnNotEmptyWhenRetrieveTheHDFSFile