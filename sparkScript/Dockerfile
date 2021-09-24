ARG ubuntu_version=18.04
FROM ubuntu:${ubuntu_version} 
FROM ubuntu:18.04

LABEL image=Spark-base-imageENV SPARK_VERSION=2.4.1
ENV HADOOP_VERSION=2.7

#install the below packages on the ubuntu imageRUN apt-get update -qq && \
RUN apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala

WORKDIR /

RUN wget --no-verbose http://www.gtlib.gatech.edu/pub/apache/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz
RUN tar -xzf /spark-2.4.1-bin-hadoop2.7.tgz && \
    mv spark-2.4.1-bin-hadoop2.7 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc

#Expose the UI Port 4040
EXPOSE 4040