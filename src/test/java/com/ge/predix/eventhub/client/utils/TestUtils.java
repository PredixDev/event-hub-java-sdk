package com.ge.predix.eventhub.client.utils;

import com.ge.predix.eventhub.Ack;
import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubLogger;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.client.Client;
import com.ge.predix.eventhub.configuration.LoggerConfiguration;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.junit.Ignore;

/**
 * This class is used for defining things used across multiple tests
 */
@Ignore
public class TestUtils {
    public static final long DEFAULT_SUBSCRIBER_TIMEOUT = 60000L;
    public static final long DEFAULT_PUBLISHER_TIMEOUT = 30000L;
    public static long SUBSCRIBER_ACTIVE_WAIT_LENGTH = 30000L;
    private static LoggerConfiguration  loggerConfig = new LoggerConfiguration.Builder().useJUL(true).logLevel(LoggerConfiguration.LogLevel.ALL).build();
    private static EventHubLogger ehLogger = new EventHubLogger(TestUtils.class, loggerConfig);


    private static AtomicInteger randomStart = new AtomicInteger(0);

    /**
     * Publish Callback for any async publishes
     */
    public static class PublishCallback implements Client.PublishCallback {
        private List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>());
        private List<Ack> acks = Collections.synchronizedList(new ArrayList<>());
        private CountDownLatch finishLatch =  new CountDownLatch(0);
        public PublishCallback() {

        }


        public void onAck(List<Ack> acks) {
            // System.out.println(String.format("%s::acks::%s::%s", name, acks.size(), acks.get(0).getStatusCode().toString()));
            this.acks.addAll(acks);
            if(finishLatch.getCount() > 0){
                for(int i=0;i<acks.size();i++)
                    finishLatch.countDown();
            }
        }

        public void block(int count){
            block(count, DEFAULT_PUBLISHER_TIMEOUT);
        }

        public void block(int count, long timeout) {
            if (getAckCount() >= count) {
                return;
            }
            finishLatch = new CountDownLatch(count - getAckCount());
            long startTime = System.currentTimeMillis();
            //System.out.println("pub block started with count of: " + finishLatch.getCount());

            try {
                finishLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(finishLatch.getCount() == 0){
                //System.out.println("publisher block finished, time: " + (System.currentTimeMillis() - startTime));
            }else{
                //System.out.println("pub block finished with count of: " + finishLatch.getCount());

            }
        }

        public void onFailure(Throwable throwable) {
            // System.out.println(name + "::error");
            // System.out.println(throwable.getMessage());
            ehLogger.log(Level.WARNING, "function", "PublishCallback.onFailure", "error", throwable);
            errors.add(throwable);
        }

        public int getAckCount() {
            return acks.size();
        }

        public List<Ack> getAcks(){
            return acks;
        }

        public int getErrorCount() {
            return errors.size();
        }

        public void resetCounts() {
            acks.clear();
            errors.clear();
        }
    }


    /**
     * The base subscriber callback, used for normal subscribers as well as for
     * the other subscribe callbacks to pass messages and errors to.
     */
    public static class TestSubscribeCallback implements Client.SubscribeCallback {
        private List<Message> messages = Collections.synchronizedList(new ArrayList<Message>());
        private List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>());
        private final String expectedMessage;
        private CountDownLatch finishLatch = new CountDownLatch(0);

        /**
         * Basic Subscribe callback
         * Use expected message so we only keep track of the messages we published for that test
         * Since most of the tests have been switched to newest should not be that much of an issue.
         *
         * @param expectedMessage the unique message published for that test
         */
        public TestSubscribeCallback(String expectedMessage) {
            this.expectedMessage = expectedMessage;
        }

        /**
         * Block the current thread for a count number of messages.
         * Used to dynamically wait for a set number of messages instead of a set time period
         * Will still timeout after the DEFAULT_SUBSCRIBER_TIMEOUT
         *
         * @param count the number of messages to wait for
         */
        public void block(int count) {
            block(count, DEFAULT_SUBSCRIBER_TIMEOUT);
        }

        /**
         * If you want to use a timeout other than the DEFAULT_SUBSCRIBER_TIMEOUT
         *
         * @param count   the number of messages to wait for
         * @param timeout the time period tom wait before we timeout
         */
        void block(int count, long timeout) {
            if (getMessageCount() >= count) {
                return;
            }
            finishLatch = new CountDownLatch(count - getMessageCount());
            long startTime = System.currentTimeMillis();
            //System.out.println("Subscriber block started with count of: " + finishLatch.getCount());

            try {
                finishLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(finishLatch.getCount() == 0){
               // System.out.println("Subscriber block finished, time: " + (System.currentTimeMillis() - startTime));
            }else{
              //  System.out.println("Subscriber block finished with count of: " + finishLatch.getCount());
            }

        }

        /**
         * On each message, only count the message if it was part of the expected message
         *
         * @param message the message coming from the streamObserver in the subscriberClient
         */
        public void onMessage(Message message) {
            if (message.getBody().toStringUtf8().equals(expectedMessage)) {
                messages.add(message);
                if (finishLatch.getCount() != 0) {
                    this.finishLatch.countDown();
                }
            }
        }

        /**
         * on a failure add the throwable to the list of throwables
         * currently the tests do not use the list of errors, only the count, but its nice when debugging
         *
         * @param throwable the error thrown the stream
         */
        public void onFailure(Throwable throwable) {
            ehLogger.log(Level.WARNING, "function", "TestSubscribeCallback.onFailure", "error", throwable);

            errors.add(throwable);
        }

        /**
         * return the number of messages that matched the expected message.
         *
         * @return message count
         */
        public int getMessageCount() {
            return messages.size();
        }

        /**
         * return the current message list
         *
         * @return the message list
         */
        public List<Message> getMessage() {
            return messages;
        }

        /**
         * get the number of errors thrown the stream
         *
         * @return the size of the error list
         */
        public int getErrorCount() {
            return errors.size();
        }

        /**
         * Reset the lists to contain no message/errors
         */
        public void resetCounts() {
            messages.clear();
            errors.clear();
        }
    }

    public static class TestSubscribeAckCallback implements Client.SubscribeCallback {
        Client subscribeClient;
        TestSubscribeCallback nonAckCallback;

        /**
         * Use expected message so we only keep track of the messages we published for that test
         * Since most of the tests have been switched to newest should not be that much of an issue.
         *
         * @param expectedMessage the unique message published for that test
         * @param subscribeClient the subscribe client to send acks to
         */
        public TestSubscribeAckCallback(String expectedMessage, Client subscribeClient) {
            nonAckCallback = new TestSubscribeCallback(expectedMessage);
            this.subscribeClient = subscribeClient;
        }

        /**
         * block the thread for a set number of messages that matched the expected message
         * uses the block() of the normal subscribe callback
         *
         * @param count the number of message to wait for
         */
        public void block(int count) {
            nonAckCallback.block(count);
        }

        /**
         * On a message we want to pass the message to the normal subscribe callback to
         * trigger the count down latch, also want to ack any message we get. This is because
         * if we don't event hub service may never send the message we were expecting
         *
         * @param message the message coming from the streamObserver in the subscriberClient
         */
        public void onMessage(Message message) {
            //System.out.println(message.toString());
            nonAckCallback.onMessage(message);
            try {
                //System.out.println("acking");
                subscribeClient.sendAck(message);
            } catch (EventHubClientException e) {
                //System.out.println(e.getMessage());
            }

        }

        /**
         * on a failure pass the throwable to the normal subscriber onFailure()
         *
         * @param throwable
         */
        public void onFailure(Throwable throwable) {
//      System.out.println(name + "::error");
//      System.out.println(throwable.getMessage());
            ehLogger.log(Level.WARNING, "function", "TestSubscribeAckCallback.onFailure", "error", throwable);

            nonAckCallback.onFailure(throwable);
        }

        /**
         * return the number of messages in the normal subscriber callback that matched the expected message
         *
         * @return number of messages passed to normal callback
         */
        public int getMessageCount() {
            return nonAckCallback.getMessageCount();
        }

        public List<Message> getMessage() {
            return nonAckCallback.getMessage();
        }

        public int getErrorCount() {
            return nonAckCallback.getErrorCount();
        }

        public void resetCounts() {
            nonAckCallback.resetCounts();
        }
    }

    /**
     * Subscribe callback for batch subscriptions
     */
    public static class TestSubscribeBatchCallback implements Client.SubscribeBatchCallback {
        Client subscribeClient;
        TestSubscribeCallback normalCallback;
        AtomicInteger batchCount;

        /**
         * Subscribe batch callback
         *
         * @param expectedMessage the
         */
        public TestSubscribeBatchCallback(String expectedMessage) {
           this(expectedMessage, null);
        }

        /**
         * Subscribe batch callback that also acks the messages to the subscribeClient provided
         * @param expectedMessage
         * @param subscribeClient
         */
        public TestSubscribeBatchCallback(String expectedMessage, Client subscribeClient){
            this.normalCallback = new TestSubscribeCallback(expectedMessage);
            this.subscribeClient = subscribeClient;
            batchCount = new AtomicInteger(0);
        }

        /**
         * On batch of messages we want to pass all the messages to the normal subscribe callback to
         * trigger the count down latch, also want to ack any message we get if we need to.
         * This is because if we don't event hub service may never send the message we were expecting
         *
         * @param messages the batch of messages coming from the streamObserver in the subscriberClient
         */
        public void onMessage(List<Message> messages) {
            batchCount.incrementAndGet();
//            System.out.println(messages.size());
            if(subscribeClient != null){
                try {
                    subscribeClient.sendAcks(messages);
                } catch (EventHubClientException e) {
                    e.printStackTrace();
                }
            }
            for (Message m : messages) {
                normalCallback.onMessage(m);
            }
        }

        public void onFailure(Throwable throwable) {
//      System.out.println(name + "::error");
//        System.out.println(throwable.getMessage());
            ehLogger.log(Level.WARNING, "function", "TestSubscribeBatchCallback.onFailure", "error", throwable);
            normalCallback.onFailure(throwable);
        }

        public void block(int count) {
            normalCallback.block(count);
        }

        public int getMessageCount() {
            return normalCallback.getMessageCount();
        }

        public int getBatchCount(){
            return batchCount.get();
        }

        public List<Message> getMessage() {
            return normalCallback.getMessage();
        }

        public int getErrorCount() {
            return normalCallback.getErrorCount();
        }

        public void resetCounts() {
            batchCount.set(0);
            normalCallback.resetCounts();

        }
    }

    /**
     * Build a random string with preface of the current random number
     * the preface is Used to easily compare a set of messages apart
     *
     * @return random string
     */
    public static String createRandomString() {
        return createRandomString(20);
    }


    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static SecureRandom rnd = new SecureRandom();
    /**
     * Build a random string with preface of the current random number
     * the preface is Used to easily compare a set of messages apart
     *
     * @return random string
     */
    public static String createRandomString(int size) {
        StringBuilder sb = new StringBuilder( size );
        for( int i = 0; i < size; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }

    public static List<Ack> buildAndSendMessage( Client publishClient, String messageBody, String messageID) throws EventHubClientException {
        return publishClient.addMessage(messageID, messageBody, null).flush();
    }

    public static List<Ack> buildAndSendMessages(Client client, String messageBody, int numberOfMessages) throws EventHubClientException {
        return buildAndSendMessages(client, messageBody, numberOfMessages, createRandomString());
    }

    public static List<Ack> buildAndSendMessages(Client client, String messageBody, int numberOfMessages, String idPreface) throws EventHubClientException {
        String id;
        for (int i = 0; i < numberOfMessages; i++) {
            id = Integer.toString(i);
            client.addMessage(idPreface + "-" + id, messageBody, null);
        }
        return client.flush();
    }


}
