package com.ge.predix.eventhub.sample.quickstart;

import com.ge.predix.eventhub.Ack;
import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubUtils;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by williamgowell on 11/28/17.
 * This class is to serve as a basic example for using the event hub sdk
 *
 */
class QuickStart{

    /**
     * Inside the callbacks we add the messages/acks to queus because
     * this way the program can process them on their own time and not block the
     * receive queues of the sdk. If these messages do take awhile to process then the overall
     * throughput of the system would be affected.
     */
    static class SubCallback implements Client.SubscribeCallback {
        ConcurrentLinkedQueue<Message> processMsgQueue;
        SubCallback() {
            processMsgQueue = new ConcurrentLinkedQueue<Message>();
        }
        @Override
        public void onMessage(Message message) {
            processMsgQueue.add(message);
        }
        @Override
        public void onFailure(Throwable throwable){
            System.out.println(throwable.toString());
        }
    }

    static class PubCallback implements Client.PublishCallback{
        ConcurrentLinkedQueue<Ack> processAckQueue;
        PubCallback(){
            processAckQueue = new ConcurrentLinkedQueue<Ack>();
        }
        @Override
        public void onAck(List<Ack> list) {
            processAckQueue.addAll(list);
        }

        @Override
        public void onFailure(Throwable throwable) {
            System.out.println(throwable.toString());
        }
    }

    public static void main(String[] args){
        EventHubConfiguration config = null;
        try {
            config = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName("my-unique-subscriber").build())
                    .build();
        } catch (EventHubClientException.InvalidConfigurationException e) {
            e.printStackTrace();
        }

        Client eventHubClient = new Client(config);
        SubCallback mySubCallback = new SubCallback();
        PubCallback myPubCallback = new PubCallback();

        try {
            eventHubClient.subscribe(mySubCallback);
            eventHubClient.registerPublishCallback(myPubCallback);
        } catch (EventHubClientException e) {
            e.printStackTrace();
        }

        int idCount = 0;
        while (true){
            try {
                eventHubClient.addMessage("id-"+idCount, "body", null).flush();
                Thread.sleep(3000);
            } catch (EventHubClientException | InterruptedException e) {
                e.printStackTrace();
            }
            while(!mySubCallback.processMsgQueue.isEmpty()){
                System.out.println(EventHubUtils.makeJson(mySubCallback.processMsgQueue.remove()).toString());
            }
            while(!myPubCallback.processAckQueue.isEmpty()){
                System.out.println(EventHubUtils.makeJson(myPubCallback.processAckQueue.remove()).toString());
            }
        }
    }
}