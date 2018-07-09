#!/bin/bash

rm -r output
rm messages.txt
mvn clean install
java -jar target/event-hub-java-spark-sample-app-1.0-SNAPSHOT.jar --server.port=58621