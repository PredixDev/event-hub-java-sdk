package com.ge.predix.eventhub.sample.stomp;

import com.ge.predix.eventhub.Message;

import java.util.Map;

/**
 * Created by williamgowell on 11/16/17.
 * POJO class representing a message sent from EventHub
 */
public class SubscribeMessage {
    private Map<String, String> tags;
    private String id, body, topic;
    private int offset, partition;

    public Map<String, String> getTags() {
        return tags;
    }

    public String getId() {
        return id;
    }

    public String getBody() {
        return body;
    }

    public String getTopic() {
        return topic;
    }

    public int getOffset() {
        return offset;
    }

    public int getPartition() {
        return partition;
    }


    SubscribeMessage(){

    }

    SubscribeMessage(Message message){
        this.id = message.getId();
        this.body = message.getBody().toStringUtf8();
        this.offset = (int) message.getOffset();
        this.tags= message.getTags();
        this.topic =  message.getTopic();
        this.partition = message.getPartition();

    }
}
