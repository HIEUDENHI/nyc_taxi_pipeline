FROM spark-base:latest

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_UI_PORT=8082
ENV SPARK_MASTER_LOG_DIR=/var/logs/spark

# Cài đặt Jupyter Notebook và các dependencies cần thiết
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install jupyter && \
    pip3 install findspark && \
    pip3 install pyspark==3.3.0  # Phiên bản PySpark tương thích với Spark 3.1.2

COPY start-master.sh .

RUN chmod +x start-master.sh

# Expose các cổng cần thiết cho Jupyter và Spark
EXPOSE 8082 
EXPOSE 7077 
EXPOSE 6066
EXPOSE 8889

# Khởi chạy cả Spark Master và Jupyter Notebook
CMD ["sh", "-c", "./start-master.sh & jupyter notebook --ip=0.0.0.0 --port=8889 --no-browser --allow-root"]
