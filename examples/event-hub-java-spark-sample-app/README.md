# event-hub-java-spark-sample-app

## Background:
Built on top of EventHub Java Sample App (https://github.com/PredixDev//predix-event-hub-java-sdk/examples) but this app adds a simple Spark job.

## Summary:
Writes all received messages in the Subscriber Callback to a file.

A GET request to "/spark" will trigger a simple Spark job that counts how many times unique words appear in the written file of received messages.

The results of the Spark job are then written to "output/part-00000".


[![Analytics](https://ga-beacon.appspot.com/UA-82773213-1/predix-event-hub-sdk/readme?pixel)](https://github.com/PredixDev)