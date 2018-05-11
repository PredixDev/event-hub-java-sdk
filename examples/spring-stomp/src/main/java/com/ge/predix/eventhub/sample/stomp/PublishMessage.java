package com.ge.predix.eventhub.sample.stomp;

/**
 * Created by williamgowell on 11/16/17.
 * POJO class for repressing a message to be published.
 */
public class PublishMessage {

    private String body;
    private static int idCount = 0;
    private String id;

    public PublishMessage(){
        this.id ="id-"+(++idCount);
    }

    public PublishMessage(String body){
        this.body = body;
        this.id ="id-"+(++idCount);
    }

    public String getBody(){
        return this.body;
    }

    public String getId(){
        return this.id;
    }

    public void setBody(String body){
        this.body = body;
    }


}
