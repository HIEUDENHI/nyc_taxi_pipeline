#!/bin/bash

. "$SPARK_HOME/sbin/spark-config.sh"
. "$SPARK_HOME/bin/load-spark-env.sh"

mkdir -p $SPARK_HISTORY_LOG_DIR

ln -sf /dev/stdout $SPARK_HISTORY_LOG_DIR/spark-history.out

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.history.HistoryServer >> $SPARK_HISTORY_LOG_DIR/spark-history.out