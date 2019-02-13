# event-hub-java-spark-sample-app

## Background:
Built on top of EventHub Java Sample App (https://github.com/PredixDev/event-hub-java-sample-app) but this app adds a simple Spark job.

## Summary:
Writes all received messages in the Subscriber Callback to a file.

A GET request to "/spark" will trigger a simple Spark job that counts how many times unique words appear in the written file of received messages.

The results of the Spark job are then written to "output/part-00000".
[![Analytics](https://predix-beacon.appspot.com/UA-82773213-1/event-hub-java-sdk/readme?pixel)](https://github.com/PredixDev)
