function kafkaPublishMessage () {
    echo "-----------------------------"
    echo "1. Test: Kafka publish message"
    echo "-----------------------------"
    kafka-console-producer --broker-list localhost:9092 --topic "station_data_test" < resources/valid_response_citybik.json
    if [ $? -eq 0 ]
    then
        echo "Failure: Cant publish message in the topic in Kafka"
        exit 1
    else
        echo "Sucess: publish message to Kafka"
    fi

}

function waitForHDFSUpdate () {
    echo "-----------------------------"
    echo "2. Test: Wait for HDFS Update"
    echo "-----------------------------"

}

echo "-----------------------------"
echo "TwoWheelers E2E Test"
echo "-----------------------------"

kafkaPublishMessage
waitForHDFSUpdate