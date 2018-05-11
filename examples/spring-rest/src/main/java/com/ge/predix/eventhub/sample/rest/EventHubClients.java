/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.predix.eventhub.sample.rest;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.google.protobuf.ByteString;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;


@RestController
@EnableAutoConfiguration
public class EventHubClients {
    private static ArrayList<Client> clients = new ArrayList<Client>();
    private static EventHubConfiguration eventHubConfiguration;

    private static final int MAX_MESSAGE_QUEUE_SIZE = 500;
    private static AtomicInteger messageCount = new AtomicInteger();
    private static final List<MessageWithClientID> rxMessages = Collections.synchronizedList(new ArrayList<MessageWithClientID>());

    private static final int MAX_ERROR_COUNT = 100;
    private static final List<String> rxErrors = Collections.synchronizedList(new ArrayList<String>());
    private static AtomicInteger rxErrorCount = new AtomicInteger();

    private static final String subscriberName = "rest-app-subscriber";
    private static final String subscriberInstance = "sample-app-instance";

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(EventHubClients.class);
    private static String zoneID;


    /**
     * Define the subscriber callback used for the clients
     * Use clientNumber to keep track of what client this is
     * connected to.
     */
    class SubCallback implements Client.SubscribeCallback {
        private int clientNumber;

        SubCallback(int clientID) {
            this.clientNumber = clientID;
        }

        public void onMessage(Message message) {
            synchronized (rxMessages){
                if(rxMessages.size() < MAX_MESSAGE_QUEUE_SIZE) {
                    rxMessages.add(new MessageWithClientID(clientNumber, message));
                }else{
                    logger.warn("Message lost, queue full\n"+message.toString());

                }
            }
        }

        public void onFailure(Throwable throwable) {
            synchronized (rxErrors){
                if(rxErrors.size() < MAX_ERROR_COUNT){
                    rxErrors.add(throwable.getMessage());
                }else{
                    logger.warn("rxErrorQueueFull");
                    logger.error(throwable.getLocalizedMessage());
                }
            }
        }
    }

    class MessageWithClientID {
        int clientId;
        Message m;

        MessageWithClientID(int clientId, Message m) {
            this.clientId = clientId;
            this.m = m;
        }
    }



    /**
     * Init the Clients
     * Start up the web socket publisher
     * Define the callback and subscribe to the topic
     *
     * @throws Exception generic exception
     */
    @PostConstruct
    public void makeClients() throws Exception {
        // Start the event hub grpc sdk
        //going to make three event hub clients

        //Provide the names of the topic.
        //This must match the topics that have been created in event hub service
        String defaultTopic = "topic";
        String topicSuffix1 = "NewTopic1";
        String topicSuffix2 = "NewTopic2";

        // The deafult topic must be added to the list of topics
        // so the subscriber knows

        List<String> topicSuffixes = new ArrayList<String>();
        topicSuffixes.add(defaultTopic);
        topicSuffixes.add(topicSuffix1);
        topicSuffixes.add(topicSuffix2);


        //Build the EventHubConfigurations for the clients
        try {

            // Subscribe only to three topics: default, NewTopic1, NewTopic2
            EventHubConfiguration configuration_0 = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .topics(topicSuffixes) //Note the topics(List) vs topic(String)
                            .subscriberName("multipleTopicSubscriber")
                            .subscriberInstance(subscriberInstance)
                            .build())
                    .build();

            // Subscribe and publish to the default topic
            // note the lack of .topic in the configuration
            EventHubConfiguration configuration_1 = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder()
                            .publisherType(PublishConfiguration.PublisherType.SYNC)
                            .timeout(3000)
                            .build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .subscriberName("defaultTopicSubscriber")
                            .subscriberInstance(subscriberInstance)
                            .build())
                    .build();


            // Subscribe and publish to topic 1
            EventHubConfiguration configuration_2 = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .topic(topicSuffix1)
                            .subscriberName("newTopic1Subscriber")
                            .subscriberInstance(subscriberInstance)
                            .build())
                    .publishConfiguration(new PublishConfiguration.Builder()
                            .publisherType(PublishConfiguration.PublisherType.SYNC)
                            .topic(topicSuffix1)
                            .timeout(2000)
                            .build())
                    .build();

            // Subscribe and publish to topic 2

            EventHubConfiguration configuration_3 = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .topic(topicSuffix2)
                            .subscriberName("newTopic2Subscriber")
                            .subscriberInstance(subscriberInstance)
                            .build())
                    .publishConfiguration(new PublishConfiguration.Builder()
                            .topic(topicSuffix2)
                            .publisherType(PublishConfiguration.PublisherType.SYNC)
                            .timeout(2000)
                            .build())
                    .build();



            clients.add(new Client(configuration_0));
            clients.add(new Client(configuration_1));
            clients.add(new Client(configuration_2));
            clients.add(new Client(configuration_3));

            for (int i = 0; i < clients.size(); i++) {
                // create and assoiate the subscribe callback classes
                clients.get(i).subscribe(new SubCallback(i));
            }

        } catch (EventHubClientException.InvalidConfigurationException e) {
            logger.info(e.getMessage());
            logger.error("could not make clients");
        }
    }


    /**
     * Home mapping to check if app is running easily
     *
     * @return Hello Event Hub String
     */
    @RequestMapping("/")
    String home() {
        return "Hello Event Hub!";
    }


    /**
     * Publish the message a count number of times
     *
     * @param input    what gets placed into the body of the message
     * @param id       the id to use for the publish message
     * @param count    the number of times to publish that message, will append number to ID
     * @param topicNum the current topic, if left blank will publish to both topics
     * @return String of acks
     * @throws EventHubClientException
     */
    @RequestMapping(value = "/publish", method = RequestMethod.POST)
    String publish(@RequestBody String input,
                   @RequestParam(value = "id") String id,
                   @RequestParam(value = "count", required = false) Integer count,
                   @RequestParam(value = "topicNum", required = false) Integer topicNum) throws EventHubClientException {
        List<Ack> acks = new ArrayList<Ack>();

        if (count == null || count <= 1) {
            count = 1;
        }

        Map<String, String> tags = new HashMap<String, String>();
        tags.put("publishType", "grpc");

        // Build the message that contains the whole list of messages to be published
        // the acutal GRPC Messages object is built
        Messages.Builder msgBuilder = Messages.newBuilder();
        for(int i=0;i<count;i++) {
            Message msg = Message.newBuilder().setId(String.format("%s-%d", id, i))
                    .setZoneId(zoneID)
                    .setBody(ByteString.copyFromUtf8(input))
                    .putAllTags(tags)
                    .build();
            msgBuilder.addMsg(msg);
        }



        // decide what clients to publish to
        // Since we set these clients as sync publishers, they will block
        // until the ack is received or the timeout value in the configuration is reached

        if (topicNum == null || topicNum == 0) {
            logger.info("sending messages to deafult topic");
            clients.get(1).addMessages(msgBuilder.build());
            acks.addAll(clients.get(1).flush());
        }

        if (topicNum == null || topicNum == 1) {
            logger.info("sending messages to NewTopic1");
            clients.get(2).addMessages(msgBuilder.build());
            acks.addAll(clients.get(2).flush());
        }

        if (topicNum == null || topicNum == 2) {
            logger.info("sending messages to NewTopic2");
            clients.get(4).addMessages(msgBuilder.build());
            acks.addAll(clients.get(4).flush());
        }


        JSONArray array = new JSONArray();
        for (Ack a : acks) {
            array.put(ackToJSON(a));
        }
        return array.toString();
    }

    /**
     * Subscribe rest endpoint.
     * Empties the Queues that are populated in the callback
     * and builds a JSON String for response
     *
     * @return A JSON Array containing messages and errors
     * @throws EventHubClientException Generic EventHub Client Exception
     */
    @RequestMapping(value = "/subscribe", method = RequestMethod.GET)
    String subscribe() throws EventHubClientException {
        JSONObject responses = new JSONObject();
        JSONArray messages = new JSONArray();
        JSONArray errors = new JSONArray();

        while (rxMessages.size() != 0) {
            MessageWithClientID messageWithClientID = rxMessages.remove(0);
            messages.put(new JSONObject(String.format("{\"subscribe_client\":%d,\"topic\":\"%s\", \"id\":\"%s\", \"Body\":\"%s\", \"tags\":%s}",
                    messageWithClientID.clientId,
                    messageWithClientID.m.getTopic(),
                    messageWithClientID.m.getId(),
                    messageWithClientID.m.getBody().toStringUtf8(),
                    (new JSONObject(messageWithClientID.m.getTags())).toString())
            ));
        }

        for (String error : rxErrors) {
            errors.put(error);
        }

        responses.put("messages", messages);
        responses.put("errors", errors);

        return responses.toString();
    }


    /**
     * Convert the Ack object to a JSON Object for returning
     * Acks though a rest endpoint
     *
     * @param a Ack object to be converted
     * @return JSONObejct representation of the Ack
     */
    static JSONObject ackToJSON(Ack a) {
        JSONObject j = new JSONObject();
        if (!a.getId().equals(a.getDefaultInstanceForType().getId()))
            j.put("id", a.getId());
        j.put("status_code", a.getStatusCode());
        if (a.getStatusCode() == AckStatus.ACCEPTED) {
            j.put("offset", a.getOffset());
            j.put("partition", a.getPartition());
            j.put("topic", a.getTopic());
        } else {
            j.put("desc", a.getDesc());
        }
        return j;
    }
}