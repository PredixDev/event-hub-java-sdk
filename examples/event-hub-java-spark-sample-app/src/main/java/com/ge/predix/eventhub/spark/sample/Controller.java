package com.ge.predix.eventhub.spark.sample;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.google.protobuf.ByteString;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    private static final String subscriberName = "java-sdk-spark-sample-app-subscriber";
    private static final String subscriberInstance = "spark-sample-app-instance";
    private static BufferedWriter writer = null;
    private static SparkConf conf = null;
    private static JavaSparkContext sc = null;


    @PostConstruct
    public void makeClient() throws Exception {
        writer = new BufferedWriter(new FileWriter("messages.txt", true));
        conf = new SparkConf().setMaster("local").setAppName("Work Count App");
        sc = new JavaSparkContext(conf);
        System.out.println("******************************* Using environment variables *******************************");
        fromExplicit();

        class SubCallback implements Client.SubscribeCallback {
            @Override
            public void onMessage(Message message) {
                rxMessages.add(message);
                try {
                    writer.write(message.toString());
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void onFailure(Throwable throwable) {
                rxErrors.add(throwable.getMessage());
                rxErrorCount.incrementAndGet();
            }
        }
        eventHub.subscribe(new SubCallback());
    }

    @PreDestroy
    void closeWriter() throws IOException {
        writer.close();
    }

    @RequestMapping(value = "/spark", method = RequestMethod.GET)
    void spark() {
        JavaRDD<String> lines = sc.textFile("messages.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("output");
        sc.close();
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
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName).subscriberInstance(subscriberInstance).build())
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
