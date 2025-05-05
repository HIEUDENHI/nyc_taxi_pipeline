FROM hive-base:latest

# Define environment variables
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3.3.1
ENV SCALA_VERSION=2.12.10
ENV SCALA_HOME=/usr/share/scala
ENV SPARK_HOME=/usr/local/spark
ENV SBT_VERSION=1.5.5
ENV PYTHONHASHSEED=1

ENV SPARK_EXECUTOR_MEMORY=6g
ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_WORKER_MEMORY=6g
ENV SPARK_DAEMON_MEMORY=6g

ENV PATH $SPARK_HOME/bin/:$PATH

# Upgrade and install tools and dependencies
RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq \
    netcat \
    apt-utils \
    curl \
    vim \
    ssh \
    net-tools \
    ca-certificates \
    jq \
    wget \
    software-properties-common

# Install Scala
WORKDIR /tmp

RUN wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar zxf scala-${SCALA_VERSION}.tgz && \
    mkdir ${SCALA_HOME} && \
    rm "scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "scala-${SCALA_VERSION}/bin" "scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf *

# Install SBT
RUN export PATH="/usr/local/sbt/bin:$PATH" && \ 
    apt-get update && \
    apt-get install ca-certificates wget tar && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt sbtVersion

# Install PySpark dependencies
RUN apt-get install -y curl python3.7 python3.7-dev python3.7-distutils
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1
RUN update-alternatives --set python /usr/bin/python3.7
RUN curl https://bootstrap.pypa.io/pip/3.7/get-pip.py | python3
RUN pip install --upgrade "pip==20.2.4"
RUN pip install numpy matplotlib scipy pandas simpy

# Install Spark
WORKDIR ${SPARK_HOME}

RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar zxf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \ 
    mv spark-${SPARK_VERSION}-bin-hadoop3/* . && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3

# Add Spark Hive library
RUN wget -O ${SPARK_HOME}/jars/spark-hive_2.12-3.3.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/3.3.0/spark-hive_2.12-3.3.0.jar

# Use Spark with Hive
COPY ./conf/spark-default.conf $SPARK_HOME/conf
COPY ./conf/hive-site.xml $SPARK_HOME/conf
RUN apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /tmp/* /var/tmp/*

WORKDIR /

COPY ./entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]