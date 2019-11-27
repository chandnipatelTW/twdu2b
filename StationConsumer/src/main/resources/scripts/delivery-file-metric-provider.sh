#!/bin/bash
FILE_PATH=/free2wheelers/stationMart/data
NAMESPACE=2Wheelers_DeliveryFile

echo "Updating cloudwatch delivery-file metrics for $FILE_PATH"
$(hdfs dfs -test -e $FILE_PATH)
FILE_EXISTS=$((1-$?))

if [ $FILE_EXISTS -eq 1 ]
then
    	echo "File exists, checking if up to date"
        LAST_MODIFIED_DIRECTORY_MS=$(hdfs dfs -stat "%Y" $FILE_PATH)
        TIME_NOW_S=$(date +%s)
        TIME_NOW_MS=$(($TIME_NOW_S*1000))

        DIFFERENCE=$((($TIME_NOW_MS-$LAST_MODIFIED_DIRECTORY_MS)/1000))
        echo "File was updated $DIFFERENCE seconds ago"
        FILE_UPDATED=$(($DIFFERENCE<600))
else
    	echo "File does not exist"
        FILE_UPDATED=0
fi

echo "Metrics calculated: FILE_EXISTS: $FILE_EXISTS, FILE_UPDATED: $FILE_UPDATED"

echo "Pushing updated metrics to cloudwatch, namespace $NAMESPACE"
aws cloudwatch put-metric-data --metric-name file-exists --region="ap-southeast-1" --dimensions Instance=j-3GR8OE0FH0VLQ --namespace "$NAMESPACE" --value $FILE_EXISTS
aws cloudwatch put-metric-data --metric-name file-updated --region="ap-southeast-1" --dimensions Instance=j-3GR8OE0FH0VLQ --namespace "$NAMESPACE" --value $FILE_UPDATED