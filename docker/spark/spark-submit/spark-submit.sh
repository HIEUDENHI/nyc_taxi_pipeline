bin/spark-submit \
--class ${SPARK_APPLICATION_MAIN_CLASS} \
--master ${SPARK_MASTER_URL} \
--deploy-mode client \
--total-executor-cores 4 \
 ${SPARK_SUBMIT_ARGS} \
 ${SPARK_APPLICATION_JAR_LOCATION} \
 ${SPARK_APPLICATION_ARGS} \