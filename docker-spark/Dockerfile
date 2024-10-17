FROM apache/airflow:2.1.2
USER 0
RUN sed -i 's/archive.ubuntu.com/mirror.kakao.com/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk
RUN apt-get install -y wget
RUN wget https://mirror.navercorp.com/apache/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -O spark.tgz
RUN tar -xvf spark.tgz && rm spark.tgz
RUN mv spark-3.1.2-bin-hadoop3.2/ spark
RUN mv spark /opt/
ENV SPARK_HOME /opt/spark
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH ${PATH}:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
USER airflow
ADD chart-etl.py /opt/airflow
ADD patient-etl.py /opt/airflow
ADD submit.sh /opt/airflow
ADD develop_thrift.sh /opt/airflow
