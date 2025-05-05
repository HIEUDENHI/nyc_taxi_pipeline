FROM spark-base:latest

ENV SPARK_HISTORY_PORT=18080
ENV SPARK_HISTORY_LOG_DIR=/var/logs/spark

COPY start-history.sh .

RUN chmod +x start-history.sh

EXPOSE 18080

CMD ["./start-history.sh"]