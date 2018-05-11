package com.ge.predix.eventhub.sample.stomp;

/**
 * Created by williamgowell on 11/30/17.
 */
public class Heartbeat {
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    long timestamp;
}
