WAITING_TIME_SECONDS=60

function kafkaPublishMessage () {
    echo "============================="
    echo "1. Test: Kafka publish message"
    echo "============================="
    pwd
    kafka-console-producer --broker-list localhost:9092 --topic "station_data_test" < 'resources/valid_response_citybik.json'
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
