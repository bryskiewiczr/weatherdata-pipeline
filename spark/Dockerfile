FROM bitnami/spark:3.5.1
USER root
RUN apt-get update && apt-get install -y wget
ENV SPARK_HOME=/opt/bitnami/spark
ENV SPARK_WORKER_OPTS="-Xss512m"
ENV SPARK_DRIVER_OPTS="-Xss512m"
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -P $SPARK_HOME/jars
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar -P $SPARK_HOME/jars
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.5.1/spark-streaming-kafka-0-10-assembly_2.12-3.5.1.jar -P $SPARK_HOME/jars
RUN wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P $SPARK_HOME/jars
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar -P $SPARK_HOME/jars
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/spark/run.sh" ]