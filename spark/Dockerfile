FROM bitnami/spark
USER root
RUN apt-get update && apt-get install -y curl
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar
RUN curl -o /opt/bitnami/spark/jars/kafka-clients-2.7.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.7.0/kafka-clients-2.7.0.jar
RUN curl -o /opt/bitnami/spark/jars/commons-pool2-2.8.0.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.8.0/commons-pool2-2.8.0.jar
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/spark/run.sh" ]