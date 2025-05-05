FROM hive-base:latest

COPY startup.sh .
ENV PYTHON_CMD=/usr/bin/python

RUN chmod +x startup.sh \
    && rm -f /opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.30.jar

EXPOSE 50111

CMD ["./startup.sh"]
