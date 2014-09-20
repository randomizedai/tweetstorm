#!/bin/bash

#first argument: input_file
hadoop fs -put $1  /user/hdfs/data_processing/data_collection/urls/to_download/