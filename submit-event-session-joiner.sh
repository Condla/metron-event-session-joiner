#!/bin/env bash

export SPARK_MAJOR_VERSION=2

export JAR=event-session-joiner-2.2.0-jar-with-dependencies.jar

export CONFIG_FILE="hdfs:///apps/metron/eventsessionjoiner/config.yml"

cp /usr/hcp/current/metron/client_jaas.conf ./



# the proofpoint jar is a spark scala application

spark-submit --principal metron@MYDOMAIN.COM \

             --keytab /etc/security/keytabs/metron.headless.keytab \

             --files ./client_jaas.conf#./client_jaas.conf \

             --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./client_jaas.conf" \

             --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./client_jaas.conf" \

             --class com.cloudera.EventSessionJoiner \

             --master yarn \

             --deploy-mode cluster \

             --num-executors 8 \

             --executor-memory 4G \

             --jars $JAR \

             $JAR $CONFIG_FILE