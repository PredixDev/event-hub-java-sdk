package com.ge.predix.eventhub.sample.stomp;

import com.ge.predix.eventhub.Ack;
import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.LoggerConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by william gowell on 11/16/17.
 * This is a sample app for demonstrating the use of Event Hub Java SDK
 * The app is configured to use STOMP messaging over WebSocket to communicate with the frontend
 *
 */
@Controller
@Service
public class EventHubClient {
    @Autowired
    private SimpMessagingTemplate template;
    private static Client eventHubClient;
    private static AtomicInteger connectionCount;
    private static Logger logger = LoggerFactory.getLogger(EventHubClient.class);
    EventHubClient() throws EventHubClientException {
        // Create the client configuration object
        // for this example we are using just the default publisher and subscriber config
        // we also are inside a spring app so we want to pipe all the logs though slf4j

        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();

        // Keep track of the number of connections
        // start the client when a user connects and disconnect when
        // all users are gone
        connectionCount = new AtomicInteger();

        // Build the client and register each of the callbacks
        eventHubClient = new Client(configuration);
        eventHubClient.registerPublishCallback(publishClallback);
        eventHubClient.subscribe(subscribeCallback);
        // Sleep to makes sure everything gets running correctly
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Shutdown the client at start.
        eventHubClient.shutdown();
    }

    /**
     * Gets called when connection count goes from 0 -> 1
     * call the reconnect function of the client to rebuild the streams
     */
    private static synchronized void startClient(){
        try {
            eventHubClient.reconnect();
        } catch (EventHubClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * Shutdown the client when the connection count goes to 0
     */
    private static synchronized void stopClient(){
        eventHubClient.shutdown();
    }

    /**
     * Called when a user disconnects from the server
     */
    public static synchronized void clientDisconnect(){
        if(connectionCount.get() > 0){
            connectionCount.decrementAndGet();
        }
        if(connectionCount.get() == 0){
            stopClient();
        }
    }

    /**
     * Called when a user connects to the server
     */
    public static synchronized void clientConnect(){
        if(connectionCount.get() == 0){
            startClient();
        }
        connectionCount.incrementAndGet();
    }

    /**
     * Callback for async receiving of acks
     */
    private Client.PublishCallback publishClallback = new Client.PublishCallback() {
        @Override
        public void onAck(List<Ack> list) {
            for(Ack a : list) {
                template.convertAndSend("/topic/publishAck", new PublishAck(a));
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            throwable.printStackTrace();
        }
    };

    /**
     * Callback for async receive of Subscription messages.
     */
    private Client.SubscribeCallback subscribeCallback = new Client.SubscribeCallback() {
        @Override
        public void onMessage(Message message) {
            template.convertAndSend("/topic/subscribeMessage", new SubscribeMessage(message));
        }

        @Override
        public void onFailure(Throwable throwable) {
            throwable.printStackTrace();
        }
    };

    /**
     * called when the user sends a message from the frontend to server
     * Take the generaeted POJO and add it to the queue
     * @param publishMessage message that was sent from the client to be published
     * @throws EventHubClientException
     */
    @MessageMapping("/publishMessage")
    public void publisMessage(PublishMessage publishMessage) throws EventHubClientException {
        eventHubClient.addMessage(publishMessage.getId(), publishMessage.getBody(), null).flush();
    }

    @MessageMapping("/heartbeat")
    public void hearthbeat(Heartbeat heartbeat){
        logger.debug("got heartbeat "+  heartbeat.getTimestamp());
    }


}
