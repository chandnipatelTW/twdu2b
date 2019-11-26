WAITING_TIME_SECONDS=60

JSON_DATA_TEST=$(cat <<EOF
{
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
		"stations": [{
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
		}]
	}
}
EOF
)

function kafkaPublishMessage () {
    echo "============================="
    echo "1. Test: Kafka publish message"
    echo "============================="
    kafka-console-producer --broker-list localhost:9092 --topic "station_data_test" < "$JSON_DATA_TEST"
    if [ $? -eq 1 ]
    then
        echo "Failure: Cant publish message in the topic in Kafka"
        exit 1
    else
        echo "Sucess: publish message to Kafka"
    fi

}

function waitProcessToFinish () {
    echo "-----------------------------"
    echo "Waiting 1 minute for process to finish"
    echo "-----------------------------"
    sleep "$WAITING_TIME_SECONDS"

}

echo "============================="
echo "TwoWheelers E2E Test"
echo "============================="


kafkaPublishMessage
waitForHDFSUpdate
