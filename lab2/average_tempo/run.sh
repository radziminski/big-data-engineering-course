#!/bin/bash

# config
JAR_FILE="wordcount.jar"
HDFS_FILE_IN="example"
HDFS_FILE_OUT=words$(date '+%Y%m%d-%H%M%S')
HADOOP_PATH='$HADOOP_HOME/bin/hadoop'
MAIN_CLASS="fh.its.bde.wordcount.WordCountApp"

# echo "Copy jar to server ..."
# scp "./target/${JAR_FILE}" "${CONNECT_STRING}:~/." || exit 1

echo "starting application ..."
"${HADOOP_PATH} jar ${JAR_FILE} ${MAIN_CLASS} ${HDFS_FILE_IN} ${HDFS_FILE_OUT}"
