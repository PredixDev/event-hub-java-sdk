package com.ge.predix.eventhub.sample;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.google.protobuf.ByteString;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@RestController
@EnableAutoConfiguration
public class Controller {
    private static final Logger logger = Logger.getLogger(Controller.class.getName());
    private static Client eventHub;
    private static EventHubConfiguration eventHubConfiguration;
    private static List<Message> rxMessages = Collections.synchronizedList(new ArrayList<Message>());
    private static List<String> rxErrors = Collections.synchronizedList(new ArrayList<String>());
    private static AtomicInteger rxErrorCount = new AtomicInteger();

    private static final String subscriberName = "java-sdk-sample-app-subscriber";
    private static final String subscriberInstance = "sample-app-instance";
    private ArrayList<String> topicList = new ArrayList<String>() {{
        add("NewTopic1");
        add("topic");
    }};

    @PostConstruct
    public void makeClient() throws Exception {
        if(System.getenv("EVENTHUB_INSTANCE_NAME") != null && System.getenv("UAA_INSTANCE_NAME") != null) {
            System.out.println("******************************* Using bound services *******************************");
            fromEnv();
        } else {
            System.out.println("******************************* Using environment variables *******************************");
            fromExplicit();
        }

        class SubCallback implements Client.SubscribeCallback {
            @Override
            public void onMessage(Message message) {
                rxMessages.add(message);
            }
            @Override
            public void onFailure(Throwable throwable) {
                rxErrors.add(throwable.getMessage());
                rxErrorCount.incrementAndGet();
            }
        }
        eventHub.subscribe(new SubCallback());
    }

    /**
     * Initialize the EventHubConfiguration Object
     * As if it were in cloud foundry (Uses VCAP services)
     * @throws EventHubClientException Exception occurred during creation
     */
    private void fromEnv() throws EventHubClientException {
        try {
            EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).topic("NewTopic1").build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName).subscriberInstance(subscriberInstance).subscribeRecency(SubscribeConfiguration.SubscribeRecency.OLDEST).topics(topicList).build())
                    .build();

            eventHub = new Client(configuration);
            logger.info("** logging Client details **");
            logger.info(configuration.getAuthURL());
            logger.info(configuration.getClientID());
            logger.info(configuration.getClientSecret());
            logger.info(configuration.getHost());
            logger.info(configuration.getZoneID());
            logger.info(configuration.getPort() + "");
            logger.info(configuration.isAutomaticTokenRenew() + "");
            logger.info("** logging Client details **");
            eventHub.forceRenewToken();
            eventHubConfiguration = configuration;

        } catch (EventHubClientException.InvalidConfigurationException e){
            logger.info(e.getMessage());
            System.out.println("Could not create client");
        }
    }


    private void fromExplicit() throws EventHubClientException {
        try {
            EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                    .host(System.getenv("EVENTHUB_URI"))
                    .port(Integer.parseInt(System.getenv("EVENTHUB_PORT")))
                    .authURL(System.getenv("AUTH_URL"))
                    .clientID(System.getenv("CLIENT_ID"))
                    .clientSecret(System.getenv("CLIENT_SECRET"))
                    .zoneID(System.getenv("ZONE_ID"))
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).topic("assets").build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName).subscriberInstance(subscriberInstance).subscribeRecency(SubscribeConfiguration.SubscribeRecency.OLDEST).topics(topicList).build())
                    .build();

            eventHub = new Client(configuration);
            logger.info("** logging Client credentials **");
            logger.info(configuration.getAuthURL());
            logger.info(configuration.getClientID());
            logger.info(configuration.getHost());
            logger.info(configuration.getZoneID());
            logger.info(configuration.getPort() + "");
            logger.info(configuration.isAutomaticTokenRenew() + "");
            logger.info("** logging Client details done**");
            eventHub.forceRenewToken();
            eventHubConfiguration = configuration;

        } catch(EventHubClientException.InvalidConfigurationException e) {
            logger.info(e.getMessage());
            System.out.println("Could not create client");
        }
    }

    @RequestMapping(value = "/subscribe", method = RequestMethod.GET)
    String subscribe() throws EventHubClientException, IOException {
        JSONObject responses = new JSONObject();
        JSONArray messages = new JSONArray();
        JSONArray errors = new JSONArray();


        while(rxMessages.size() != 0) {
            Message message = rxMessages.remove(0);
            messages.put(new JSONObject(String.format("{\"id\":\"%s\", \"body\":%s}", message.getId(), message.getBody().toStringUtf8())));
        }


        for (String error : rxErrors) {
            errors.put(error);
        }

        responses.put("messages", messages);
        responses.put("errors", errors);

        return responses.toString();
    }

    @RequestMapping(value = "/publish", method = RequestMethod.POST)
    String publish(@RequestBody String input, @RequestParam(value = "id") String id, @RequestParam(value = "count", required = false) Integer count ) throws EventHubClientException {
        List<Ack> acks;
        if(count != null && count >= 1){
            Messages.Builder msgBuilder = Messages.newBuilder();
            for(int i=0;i<count;i++){
                Message msg = Message.newBuilder().setId(String.format("%s-%d", id, i))
                        .setZoneId(eventHubConfiguration.getZoneID())
                        .setBody(ByteString.copyFromUtf8(String.format("{message:\"%s\"}", input))).build();

                msgBuilder.addMsg(msg);
            }
            acks = eventHub.addMessages(msgBuilder.build()).flush();
        }
        else{
            count =  1;
            acks = eventHub.addMessage(id, input, null).flush();
        }

        JSONArray array = new JSONArray();
        for(Ack a : acks){
            array.put(ackToJSON(a));
        }
        return array.toString();
    }


    @RequestMapping("/")
    String home() {
        return "Hello Event Hub!";
    }

    private JSONObject ackToJSON(Ack a) {
        JSONObject j = new JSONObject();
        if(!a.getId().equals(a.getDefaultInstanceForType().getId()))
            j.put("id", a.getId());
        j.put("status_code", a.getStatusCode());
        if(a.getStatusCode() == AckStatus.ACCEPTED){
            j.put("offset", a.getOffset());
            j.put("partition", a.getPartition());
            j.put("topic", a.getTopic());
        }else{
            j.put("desc", a.getDesc());
        }
        return j;
    }
}
