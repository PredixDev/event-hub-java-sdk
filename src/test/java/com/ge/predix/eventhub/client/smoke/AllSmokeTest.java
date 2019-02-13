package com.ge.predix.eventhub.client.smoke;

import static com.ge.predix.eventhub.client.utils.TestUtils.SUBSCRIBER_ACTIVE_WAIT_LENGTH;
import static com.ge.predix.eventhub.client.utils.TestUtils.buildAndSendMessages;
import static com.ge.predix.eventhub.client.utils.TestUtils.createRandomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ge.predix.eventhub.Ack;
import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.client.CustomMetrics;
import com.ge.predix.eventhub.client.utils.TestUtils;
import com.ge.predix.eventhub.client.utils.TestUtils.TestSubscribeBatchCallback;
import com.ge.predix.eventhub.client.utils.TestUtils.TestSubscribeCallback;
import com.ge.predix.eventhub.configuration.Environment;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PropertiesConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.ge.predix.eventhub.test.categories.SmokeTest;

/**
 * Enable these smoke tests for production to do basic checks.
 * All the necessary key=value pairs to bootstrap your client should be part of the event-hub.properties file
 * @author Krunal
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(SmokeTest.class)
public class AllSmokeTest {
    private static final Logger logger = LoggerFactory.getLogger(AllSmokeTest.class);      

    final String subscriberName = "smoke-subscriber";
    final String subscriberID = "smokesubscriber";
    private static Properties props;
    final String environment = "test";
    
    Client syncClient;
    Client asyncClient;
    private static Map<String,String> propertyValues = new HashMap<String,String>();
  
    /**
     * Initialize the props by reading from the property file event-hub-*.properties
     * @throws Exception
     */
    static {
            PropertiesConfiguration propsConfiguration = null;
            try {
            String value = System.getenv("environment");
            Environment env = null;
            
             if (null!=value && value.equalsIgnoreCase("production"))
                 env = Environment.PRODUCTION;
             else
                 env = Environment.TEST;
             logger.info(" Environment used for testing : "+env);
             propsConfiguration = new PropertiesConfiguration(env);
             
            } catch (Exception e) {
                // TODO Auto-generated catch block
                logger.info("Error reading props file");
            }
            props = propsConfiguration.getProperties();
            props.forEach((k, v) -> propertyValues.put((String)k,(String)v));
    }

    
    private void pause(Long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            logger.info("~~~~~ TIMEOUT INTERRUPTED ~~~~~");
        }
    }

    
    
    @Before
    public void createClient() throws Exception{
        if(propertyValues.size() == 0)
            throw new Exception("Failed to create client ! Check if event-hub.properites file is populated with key=value pairs for this enviornment");
     
        String host = propertyValues.get("EVENTHUB_HOST");
        if(host==null)
            host = propertyValues.get("EVENTHUB_URI");
        
        String scope = "predix-event-hub.zones.ZONE_ID.user,predix-event-hub.zones.ZONE_ID.grpc.publish,predix-event-hub.zones.ZONE_ID.grpc.subscribe";
        scope  = scope.replaceAll("ZONE_ID", propertyValues.get("ZONE_ID"));
        logger.info("Scopes requested:"+scope);
        EventHubConfiguration syncConfiguration = new EventHubConfiguration.Builder()
                .host(host)
                .port(Integer.parseInt(propertyValues.get("EVENTHUB_PORT")))
                .zoneID(propertyValues.get("ZONE_ID"))
                .clientID(propertyValues.get("CLIENT_ID"))
                .clientSecret(propertyValues.get("CLIENT_SECRET"))
                .authURL(propertyValues.get("AUTH_URL"))
                .authScopes(scope)
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName(subscriberName+"-sync")
                        .subscriberInstance(subscriberID)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();
        
         syncClient = new Client(syncConfiguration);
         
         EventHubConfiguration asyncConfiguration = new EventHubConfiguration.Builder()
                 .host(host)
                 .port(Integer.parseInt(propertyValues.get("EVENTHUB_PORT")))
                 .zoneID(propertyValues.get("ZONE_ID"))
                 .clientID(propertyValues.get("CLIENT_ID"))
                 .clientSecret(propertyValues.get("CLIENT_SECRET"))
                 .authURL(propertyValues.get("AUTH_URL"))
                 .authScopes(scope)
                 .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                 .subscribeConfiguration(new SubscribeConfiguration.Builder()
                         .subscriberName(subscriberName+"-async")
                         .subscriberInstance(subscriberID)
                         .acksEnabled(true)
                         .build())
                 .build();
         
        
         asyncClient  = new Client(asyncConfiguration);
         
    }
    
    /**
     * This tests metrics calls by sending one message. 
     * @throws Exception
     */
   // @Test
    public void recevieMetricsTest() throws Exception{
        Client subscribeWithMetricsClient = null;
        long startTime = System.currentTimeMillis();
        
        String host = propertyValues.get("EVENTHUB_HOST");
        if(host==null)
            host = propertyValues.get("EVENTHUB_URI");
        
        try {
            logger.info(" \n\n************ Starting recevieMetricsTest ***************** !!!");
            
            String scope = "predix-event-hub.zones.ZONE_ID.user,predix-event-hub.zones.ZONE_ID.grpc.publish,predix-event-hub.zones.ZONE_ID.grpc.subscribe";
            scope  = scope.replaceAll("ZONE_ID", propertyValues.get("ZONE_ID"));
            logger.info("Scopes requested:"+scope);
            int numMessages = 1;
            CustomMetrics metrics = new CustomMetrics();

            EventHubConfiguration subscribeWithMetricsConfiguration = new EventHubConfiguration.Builder()
                    .host(host)
                    .port(Integer.parseInt(propertyValues.get("EVENTHUB_PORT")))
                    .zoneID(propertyValues.get("ZONE_ID"))
                    .clientID(propertyValues.get("CLIENT_ID"))
                    .clientSecret(propertyValues.get("CLIENT_SECRET"))
                    .authURL(propertyValues.get("AUTH_URL"))
                    .authScopes(scope)
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .subscriberName(subscriberName)
                            .subscriberInstance(subscriberID)
                            .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                            .metricsCallBack(metrics)
                            .metricsIntervalMinutes(1)
                            .metricsEnabled(true)
                            .batchSize(10)
                            .batchIntervalMilliseconds(1000)
                            .build())
                    .build();
           
            subscribeWithMetricsClient = new Client(subscribeWithMetricsConfiguration);

            String message = createRandomString();
            TestSubscribeBatchCallback subscribeBatchRateCallback = new TestSubscribeBatchCallback(message);
            subscribeWithMetricsClient.subscribe(subscribeBatchRateCallback);
            
            logger.info("Pause for subscriber");
            pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
            
            String messageBody = createRandomString();
            String messageIdPreface = createRandomString();

            // build and send messages
            List<Ack> theAcks =  buildAndSendMessages(subscribeWithMetricsClient, messageBody, numMessages, messageIdPreface);
            theAcks.forEach((ack) -> logger.info("Ack Received:"+ack.getId()+":"+ack.getOffset()+":"+ack.getPartition()));
            
            //Acks for 1 messages published
            assertEquals(1, theAcks.size());
            pause(900L);
            
            assertTrue(subscribeBatchRateCallback.getMessageCount() <= 10);
            // Wait before metric messages arrive
            pause(60000L);
            logger.info(" Messages count in metrics:"+metrics.messageCount());

            metrics.getMessages().forEach((k) -> logger.info(" Metrics message "+k));
            assertEquals(metrics.messageCount(),numMessages);
        }
        catch(Exception e) {
            logger.error(e.getMessage(),e);
            throw new EventHubClientException(e); 
        }
        finally {
            if( subscribeWithMetricsClient!=null)
                subscribeWithMetricsClient.shutdown();
            long endTime = System.currentTimeMillis();
            endTime = (endTime - startTime)/1000;
            logger.info(String.format("************ Finished recevieMetricsTest in %s secs ",endTime));

        }
    }
    
  
    /**
     * This test is a normal sync publish , subscribe test that sends #of messages based on the value configured in props file
     * @throws EventHubClientException
     */
    @Test
    public void publishSubscribeSyncTest() throws EventHubClientException {
        long startTime = System.currentTimeMillis();
       
        int numberOfMessages = 0;
        
         try { 
             numberOfMessages =  Integer.parseInt(propertyValues.get("NUMBER_OF_MESSAGES"));
         }catch(NumberFormatException e) {
             numberOfMessages = 1;
         }
         
        logger.info(" \n\n************ Starting publishSubscribeSyncTest ***************** !!!");
        logger.info(String.format(" Total number of messages/ack to be sent/receive %s",numberOfMessages));  
        try {
      
            if(syncClient == null)
                throw new Exception("Client is not initialized");
           
            String message = createRandomString();
            
            TestSubscribeCallback callback = new TestSubscribeCallback( message);
            syncClient.subscribe(callback);
            
            pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

            List<Ack> acks = buildAndSendMessages(syncClient, message, numberOfMessages);
        
            acks.forEach((ack) -> logger.info("Ack Received:"+ack.getId()+":"+ack.getOffset()+":"+ack.getPartition()));
            assertEquals(numberOfMessages, acks.size());
            
            callback.block(numberOfMessages);
            
            TestSubscribeCallback testSubscribeCallback = (TestSubscribeCallback) syncClient.getSubscribeCallback();
            assertEquals(numberOfMessages, testSubscribeCallback.getMessageCount());
 
        }
        catch(Exception e) {
            logger.info("Recevied exception during subsribe test "+e.getMessage());
            throw new EventHubClientException(e);
        }
        finally {
            long endTime = System.currentTimeMillis();
            endTime = (endTime - startTime)/1000;
            logger.info(String.format("************ Finished publishSubscribeSyncTest in %s secs ",endTime));
        }
    }
   
    /**
     * This test is a normal async publish , subscribe test that sends #of messages based on the value configured in props file
     * @throws EventHubClientException
     */
    @Test
    public void publishSubscribeAsyncTest() throws EventHubClientException {
        long startTime = System.currentTimeMillis();
       
        int numberOfMessages = 0;
        
         try { 
             numberOfMessages =  Integer.parseInt(propertyValues.get("NUMBER_OF_MESSAGES"));
         }catch(NumberFormatException e) {
             numberOfMessages = 1;
         }
         
        logger.info(" \n\n************ Starting publishSubscribeAsyncTest ***************** !!!");
        logger.info(String.format(" Total number of messages/ack to be sent/receive %s",numberOfMessages));  
        try {
      
            if(asyncClient == null)
                throw new Exception("Client is not initialized");
            
            String message = createRandomString();
            
            TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
            asyncClient.registerPublishCallback(callback);
            
            pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
            
            for(int i =0;i<numberOfMessages;i++) {
                asyncClient.addMessage("smoke-"+System.currentTimeMillis(), message, null);
            }
            
            asyncClient.flush();
            
            callback.block(numberOfMessages);
            
            List<Ack> acks = callback.getAcks();
            acks.forEach((ack) -> logger.info("Ack Received:"+ack.getId()+":"+ack.getOffset()+":"+ack.getPartition()));
            assertEquals(numberOfMessages, callback.getAckCount());
         
 
        }
        catch(Exception e) {
            logger.info("Recevied exception during subsribe test "+e.getMessage());
            throw new EventHubClientException(e);
        }
        finally {
            long endTime = System.currentTimeMillis();
            endTime = (endTime - startTime)/1000;
            logger.info(String.format("************ Finished publishSubscribeAsyncTest in %s secs ",endTime));
        }
    }
    
   @After
   public void shutdown() {
       if(syncClient!=null)
           syncClient.shutdown();
       if(asyncClient!=null)
           asyncClient.shutdown();
   }
   
  
}
