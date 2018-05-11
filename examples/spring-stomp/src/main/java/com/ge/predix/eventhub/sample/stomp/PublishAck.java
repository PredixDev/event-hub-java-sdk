package com.ge.predix.eventhub.sample.stomp;

import com.ge.predix.eventhub.Ack;

/**
 * Created by williamgowell on 11/16/17.
 * POJO class for representing a publish Ack
 */
public class PublishAck {
    private String id;
    private String status;
    private String topic;
    private int offset;
    private int partition;
    PublishAck(){

    }

    PublishAck(Ack a){
        this.id = a.getId();
        this.partition = a.getPartition();
        this.offset = (int) a.getOffset();
        this.status = a.getStatusCode().name();
        this.topic = a.getTopic();

    }

    PublishAck(String id, String status){
        this.id = id;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public String getStatus(){
        return this.status;
    }

    public String getTopic(){
        return topic;
    }

    public int getOffset(){
        return offset;
    }

    public int getPartition(){
        return partition;
    }

}
