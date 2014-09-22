#!/bin/bash

#creating directory with hour-precision timestamp, if not exist
DIRECTORY=$(date +%Y%m%d%H)
URL_DIR=/user/hdfs/data_processing/data_collection/urls/to_download/$DIRECTORY

echo $URL_DIR
hadoop fs -test -d $URL_DIR
TestDir=$?

echo $TestDir
if [ $TestDir -eq 0 ]; then
	echo "Directory exist"
else
	hadoop fs -mkdir $URL_DIR
        echo “Creating  directory”
fi

#put files in the directory
hadoop fs -put $1  $URL_DIR/