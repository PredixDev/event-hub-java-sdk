package com.ge.predix.eventhub.client;

import static com.ge.predix.eventhub.client.utils.TestUtils.SUBSCRIBER_ACTIVE_WAIT_LENGTH;
import static com.ge.predix.eventhub.client.utils.TestUtils.buildAndSendMessage;
import static com.ge.predix.eventhub.client.utils.TestUtils.buildAndSendMessages;
import static com.ge.predix.eventhub.client.utils.TestUtils.createRandomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.net.ssl.SSLException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.client.utils.TestUtils.TestSubscribeAckCallback;
import com.ge.predix.eventhub.client.utils.TestUtils.TestSubscribeBatchCallback;
import com.ge.predix.eventhub.client.utils.TestUtils.TestSubscribeCallback;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.ge.predix.eventhub.stub.Ack;
import com.ge.predix.eventhub.stub.Message;

/**
 * Created by 212571077 on 7/11/16.
 */
@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SubscribeClientTest {
    Long TIME_FOR_FIRST_RETRY = 36000L;
    int DEFAULT_RETRY_INTERVAL = 31;
    int DEFAULT_DURATION_BEFORE_RETRY = 30;

    final String subscriberName = "test-subscriber";
    final String subscriberID = "tester";
    final String subscriberNameNewest = "test-subscriber-newest";


    Client client;
    EventHubConfiguration eventHubConfiguration;

    private void pause(Long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            System.out.println("~~~~~ TIMEOUT INTERRUPTED ~~~~~");
        }
    }

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("################## Starting test ###############: " + description.getMethodName());
        }
    };

    @BeforeClass
    public static void beforeClass() {
        // Make an instance of BackOffDelay to override static delay array so it is shorter for tests
        BackOffDelayTest.MockBackOffDelay mockClient = new BackOffDelayTest.MockBackOffDelay(null);
    }

    /**
     * @throws SSLException
     */
    @Before
    public void createClient() throws Exception {
        try {
            eventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .subscribeConfiguration(
                            new SubscribeConfiguration.Builder()
                                    .subscriberName(subscriberName)
                                    .subscriberInstance(subscriberID)
                                    .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                                    .build())
                    .build();
        } catch (EventHubClientException.InvalidConfigurationException e) {
            System.out.println("*** Could not make client ***\n" + e.toString());
        }

        client = new Client(eventHubConfiguration);
    }

    /**
     * Test Basic Subscription
     * Publish 50 Messages to Event Hub and make sure you get 50 back
     *
     * @throws EventHubClientException
     */
    @Test
    public void subscribe() throws EventHubClientException {
        // clear queue
        int numberOfMessages = 50;

        String message = "subscribe test message";
        TestSubscribeCallback callback = new TestSubscribeCallback( message);
        client.subscribe(callback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        List<Ack> acks = buildAndSendMessages(client, message, numberOfMessages);

        assertEquals(numberOfMessages, acks.size());
        callback.block(numberOfMessages);
        TestSubscribeCallback testSubscribeCallback = (TestSubscribeCallback) client.getSubscribeCallback();
        assertEquals(numberOfMessages, testSubscribeCallback.getMessageCount());
    }

    /**
     * Test subscribing different names.
     * Each subscriber should get both messages since they have a unique name
     *
     * @throws EventHubClientException
     */
    @Test
    public void subscribeDifferentName() throws EventHubClientException {
        String message = "subscribeDifferentName test message";
        int numMessages = 50;
        // create another client
        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(
                        new SubscribeConfiguration.Builder()
                                .subscriberName(subscriberName + "-1")
                                .subscriberInstance(subscriberID)
                                .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                                .build())
                .build();

        Client otherClient = new Client(eventHubConfiguration);

        TestSubscribeCallback callback = new TestSubscribeCallback( message);
        TestSubscribeCallback callbackOther = new TestSubscribeCallback( message);
        client.subscribe(callback);
        otherClient.subscribe(callbackOther);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        // send messages
        List<Ack> acks = buildAndSendMessages(client, message, numMessages);
        assertEquals(numMessages, acks.size());

        // wait for messages
        // both should have gotten numMessages
        callback.block(numMessages);
        callbackOther.block(numMessages);

        // we get each message twice because each subscriber will receive a copy
        assertEquals(numMessages * 2, callback.getMessageCount() + callbackOther.getMessageCount());
        otherClient.shutdown();
    }

    /**
     * Subscribe with one callback, publish messages,
     * Switch to different callback, check that new messages go to new callback
     *
     * @throws EventHubClientException
     */
    @Test
    public void subscribeResubscribe() throws EventHubClientException {
        String message = "subscribeResubscribe test message";
        TestSubscribeCallback callback = new TestSubscribeCallback( message);
        TestSubscribeCallback callbackOther = new TestSubscribeCallback( message);
        //Subscribe and wait for subscriber to become active
        client.subscribe(callback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        int numMessages = 50;
        List<Ack> acks = buildAndSendMessages(client, message, numMessages);
        assertEquals(numMessages, acks.size());

        // wait for messages and make sure we got all of them
        callback.block(numMessages);
        assertEquals(numMessages, callback.getMessageCount());

        // switch to new callback
        client.subscribe(callbackOther);
        //now do the other callback
        List<Ack> acksOther = buildAndSendMessages(client, message, numMessages);
        assertEquals(numMessages, acksOther.size());
        callbackOther.block(numMessages);
        // make sure we get the messages
        assertEquals(numMessages, callbackOther.getMessageCount());
    }

    @Test
    // ensure that after shutdown, no messages are received
    public void subscribeEnsureShutdown() throws EventHubClientException {
        //make new pub client
        //shutdown client
        //publish and wait for messages
        String message = "subscribeEnsureShutdown test message";
        int numMessags = 50;
        // create another client
        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(
                        new SubscribeConfiguration.Builder()
                                .subscriberName(subscriberName + "-1")
                                .subscriberInstance(subscriberID)
                                .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                                .build())
                .build();

        Client otherClient = new Client(eventHubConfiguration);
        TestSubscribeCallback callback = new TestSubscribeCallback( message);

        otherClient.subscribe(callback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        otherClient.subscribeClient.shutdown("Client test closing stream");
        List<Ack> acks = buildAndSendMessages(client, message, numMessags);
        assertEquals(numMessags, acks.size());

        // wait for messages
        //Should timeout
        callback.block(numMessags);
        // stream is closed so we should get no messages
        assertEquals(0, callback.getMessageCount());

        otherClient.shutdown();
    }

    @Test
    // when two threads subscribe at the same time, only one thread will actually subscribe
    public void subscribeDifferentThreads() throws InterruptedException, EventHubClientException {
        String message = createRandomString();
        final TestSubscribeCallback callback1 = new TestSubscribeCallback( message);
        final TestSubscribeCallback callback2 = new TestSubscribeCallback(message);
        int numMessages = 4;

        // make threads
        final CountDownLatch latch = new CountDownLatch(1);
        Thread subscribe1 = new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                try {
                    client.subscribe(callback1);
                } catch (EventHubClientException e) {
                    e.printStackTrace();
                }

            }
        };
        Thread subscribe2 = new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                try {
                    client.subscribe(callback2);
                } catch (EventHubClientException e) {
                    e.printStackTrace();
                }
            }
        };

        // subscribe
        subscribe1.start();
        subscribe2.start();
        latch.countDown();
        subscribe1.join();
        subscribe2.join();

        //let whatever subscriber callback won become active before publish
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        // send messages
        List<Ack> acks = buildAndSendMessages(client, message, numMessages);
        assertEquals(numMessages, acks.size());

        // give some time for subscribes
        callback1.block(numMessages);
        //copy messages from 1 to 2
        for (Message m : callback1.getMessage()) {
            callback2.onMessage(m);
        }
        callback2.block(numMessages);

        // check that callback 2 saw all the messages
        assertEquals(numMessages, callback2.getMessageCount());
    }

    /**
     * todo breaks sub count
     *
     * @throws InterruptedException
     * @throws EventHubClientException
     */
    @Test
    @Ignore
// when unsubscribe and subscribe are called at the same time, one will win out
    public void subscribeUnsubscribeThread() throws InterruptedException, EventHubClientException {
        String message = createRandomString();
        final TestSubscribeCallback callback1 = new TestSubscribeCallback( message);

// make threads
        final CountDownLatch latch = new CountDownLatch(1);
        Thread subscribe1 = new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                try {
                    client.subscribe(callback1);
                } catch (EventHubClientException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread subscribe2 = new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                try {
                    client.unsubscribe();
                } catch (EventHubClientException e) {
                    e.printStackTrace();
                }
            }
        };

// subscribe
        subscribe1.start();
        subscribe2.start();
        latch.countDown();
        subscribe1.join();
        subscribe2.join();
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
// send messages
        List<Ack> acks = buildAndSendMessages(client, message, 4);

        assertEquals(4, acks.size());

// give some time for subscribes
        callback1.block(4);

// check subscribers ... either we get all 4 messages or the stream is closed
        assertTrue((4 == callback1.getMessageCount()) || (client.subscribeClient.isStreamClosed()));
    }

    @Test
    @Ignore
//todo breaks sub count w/o pause
    public void ifUnsubscribedNosubscribeOnReconnect() throws EventHubClientException {
        TestSubscribeCallback subCallback = new TestSubscribeCallback( "no message");
        client.subscribe(subCallback);
        assertTrue(!client.subscribeClient.isStreamClosed());
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        client.unsubscribe();
        assertTrue(client.subscribeClient.isStreamClosed());
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        client.forceRenewToken();
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        assertTrue(client.subscribeClient.isStreamClosed());
    }


    @Test
    public void subscribeWithAckMessageRetry() throws EventHubClientException {
        String thisSubscriberName = "test-subscriber-subscribeWithAckMessageRetry";
        String thisSubscriberID = "tester-subscribeWithAckMessageRetry";
//build subscribe with ack client
        int numMessages = 10;
        EventHubConfiguration subscribeWithAckConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName(thisSubscriberName + "-1")
                        .subscriberInstance(thisSubscriberID)
                        .acksEnabled(true)
                        .retryIntervalSeconds(30)
                        .durationBeforeRetrySeconds(30)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();

        String messageBody = createRandomString();
        String messageIdPreface = createRandomString();
        Client subscribeWithAckClient = new Client(subscribeWithAckConfiguration);

        TestSubscribeCallback testSubscribeCallback = new TestSubscribeCallback(messageBody);
        subscribeWithAckClient.subscribe(testSubscribeCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
// build and send messages
        buildAndSendMessages(client, messageBody, numMessages, messageIdPreface);

// wait for messages
        testSubscribeCallback.block(numMessages);
        assertEquals(numMessages, testSubscribeCallback.getMessageCount());

        List<Message> messages = testSubscribeCallback.getMessage();
        List<Message> messageBuffer = new ArrayList<Message>();
        messageBuffer.addAll(messages);
        testSubscribeCallback.resetCounts();
        Message removedMessage = null;
//remove message 1 from message buffer
        for (int i = 0; i < messageBuffer.size(); i++) {
            if (messageBuffer.get(i).getId().equals(messageIdPreface + "-1")) {
                removedMessage = messageBuffer.get(i);
                messageBuffer.remove(messageBuffer.get(i));
            }
        }
//send acks for all messages except message 1
        System.out.println(messageBuffer);
        subscribeWithAckClient.sendAcks(messageBuffer);
        messages.clear();

//wait for message 1 to be retried
        pause(30000L);
        testSubscribeCallback.block(1);
        pause(5000L);

//should have reobtained message 1 since we didn't ack it
        System.out.println("got");
        System.out.println(testSubscribeCallback.getMessage());
        subscribeWithAckClient.sendAck(removedMessage);

        assertEquals(1, testSubscribeCallback.getMessageCount());
        assertEquals(messageIdPreface + "-1", testSubscribeCallback.getMessage().get(0).getId());

        subscribeWithAckClient.shutdown();
    }


    /**
     * todo breaks subscriber count
     *
     * @throws EventHubClientException
     */
    @Test(expected = EventHubClientException.class)
    public void sendAckIllegalCall() throws EventHubClientException {

        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName(subscriberName + "-1").subscriberInstance(subscriberID + "new")
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();

//Client subscribeClient = new Client(eventHubConfiguration);
        String messageBody = createRandomString();
        TestSubscribeCallback testSubscribeCallback = new TestSubscribeCallback( messageBody);
        client.subscribe(testSubscribeCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        List<Ack> ackList = buildAndSendMessages(client, messageBody, 1);

        testSubscribeCallback.block(1);
        //Sending ack without subscribing with ack will throw EventHubClientException
        assertNotEquals(0, testSubscribeCallback.getMessage().size());
        client.sendAck(testSubscribeCallback.getMessage().get(0));
    }


    /**
     * Subscribe with no ack, then
     * This test is no longer valid in 2.0 since it is now impossible to do two diffrent subscribers
     * on the same client.
     * @throws EventHubClientException
     */
    @Test
    @Ignore
    @Deprecated
    public void subscribeThenResubscribeWithAck() throws EventHubClientException {
        int numMessages = 25;
        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName(subscriberName + "-1")
                        .subscriberInstance(subscriberID)
                        .acksEnabled(true)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();

        Client client = new Client(eventHubConfiguration);
        String messageBody = createRandomString();

        TestSubscribeCallback testSubscribeCallback = new TestSubscribeCallback( messageBody);
        client.subscribe(testSubscribeCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        buildAndSendMessages(client, messageBody, numMessages);

//wait for messages
        testSubscribeCallback.block(numMessages);

        assertEquals(numMessages, testSubscribeCallback.getMessageCount());
        client.unsubscribe();
        TestSubscribeAckCallback testSubscribeAckCallback = new TestSubscribeAckCallback(messageBody, client);
        pause(10000L);
//Resubscribe with Ack stream
        client.subscribe(testSubscribeAckCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        buildAndSendMessages(client, messageBody, numMessages);
        testSubscribeAckCallback.block(numMessages);

        assertEquals(numMessages, testSubscribeAckCallback.getMessage().size());
        assertEquals(0, testSubscribeAckCallback.getErrorCount());

        client.shutdown();
    }

    /**
     * Ensure newest subscriber works as intended.
     * Publishes numMessages (50) messages, then subscribes with newest recency.
     * waits 5s for the newest subscriber to become active in kafka, then publishes
     * numMessages2 (10) messages. The subscriber should only receive the 10 messages.
     *
     * @throws EventHubClientException if something goes wrong
     */
    @Test
    public void subscribeNewest() throws EventHubClientException {
        int numMessages = 50;
        int numMessages2 = 10;
        Client newestClient;
        EventHubConfiguration nEventHubConfiguration = null;
        try {
            nEventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .subscriberName(subscriberNameNewest +"-1")
                            .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST).build())
                    .build();
        } catch (EventHubClientException.InvalidConfigurationException e) {
            System.out.println("*** Could not make client for newest messages on subscription ***");
        }
        newestClient = new Client(nEventHubConfiguration);

        String message = "subscribe test message";
        List<Ack> acks = buildAndSendMessages(newestClient, message, numMessages);
        assertEquals(numMessages, acks.size());

//check that only messages published after subscribe are received and not all messages published so far
        message = "subscribe newest test message";
        TestSubscribeCallback nCallback = new TestSubscribeCallback( message);
        newestClient.subscribe(nCallback);
// wait for subscriber to become active (since we are newest we must wait).
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        acks = buildAndSendMessages(newestClient, message, numMessages2);
        assertEquals(numMessages2, acks.size());

        nCallback.block(numMessages2);
//this consumer should not receive all messages published so far but only the messages published after the subscription
        assertEquals(numMessages2, nCallback.getMessageCount());

        newestClient.unsubscribe();
        newestClient.shutdown();
    }

    /**
     * Test to measure how long it takes a newest subscriber to become active in kafka
     * Starts a newest subscriber and publishes 10 messages/second. We can see
     * how long it takes the subscriber to become active by seeing the id of the first message
     * received
     *
     * @throws EventHubClientException if something goes wrong
     */
    @Test
    @Ignore
    public void subscribeNewestDelayMeasure() throws EventHubClientException {
        final Client newestClient;
        EventHubConfiguration nEventHubConfiguration = null;
        try {
            nEventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder()
                            .subscriberName(subscriberNameNewest + "-1")
                            .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                            .build())
                    .build();
        } catch (EventHubClientException.InvalidConfigurationException e) {
            System.out.println("*** Could not make client for newest messages on subscription ***");
        }
        newestClient = new Client(nEventHubConfiguration);
        final String messageBody = createRandomString();

//check that only messages published after subscribe are received and not all messages published so far
//message = "subscribe newest test message";
        TestSubscribeCallback nCallback = new TestSubscribeCallback( messageBody);

        Thread publishingThread = new Thread() {
            public void run() {
                int messageID = 0;
                for (int i = 0; i < 100; i++) {
//send 1 message then wait .1 second
                    try {
                        buildAndSendMessage(newestClient, messageBody, messageID + "");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    messageID++;
                    pause(100L);
                }
            }
        };
        newestClient.subscribe(nCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        publishingThread.start();

// wait for first message
        nCallback.block(1);
        assertTrue(nCallback.getMessageCount() >= 1);
        System.out.println(nCallback.getMessage().get(0).getId());
        try {
            publishingThread.interrupt();
        } catch (Exception ignored) {
        }
//this consumer should not receive all messages published so far but only the messages published after the subscription
        newestClient.unsubscribe();
        newestClient.shutdown();
    }

    @Test
    public void sendAckInCallback() throws EventHubClientException {
//build subscribe with ack client
        EventHubConfiguration subscribeWithAckConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName(subscriberName + "-1")
                        .subscriberInstance(subscriberID)
                        .acksEnabled(true)
                        .retryIntervalSeconds(DEFAULT_RETRY_INTERVAL)
                        .durationBeforeRetrySeconds(DEFAULT_DURATION_BEFORE_RETRY)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .build();

        int numberOfMessages = 30;
        String messageBody = createRandomString();
        Client subscribeWithAckClient = new Client(subscribeWithAckConfiguration);

        TestSubscribeAckCallback testSubscribeAckCallback = new TestSubscribeAckCallback(messageBody, subscribeWithAckClient);
        subscribeWithAckClient.subscribe(testSubscribeAckCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        buildAndSendMessages(subscribeWithAckClient, messageBody, numberOfMessages);
        testSubscribeAckCallback.block(numberOfMessages);

        //should have obtained no extra messages
        assertEquals(numberOfMessages, testSubscribeAckCallback.getMessageCount());

        subscribeWithAckClient.shutdown();
    }

    @Test
    public void subscribeInBatch() throws EventHubClientException {
        int batchSize = 100;
        int batchInterval = 1000;
        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName(subscriberName + "-1")
                        .batchingEnabled(true)
                        .batchSize(batchSize)
                        .batchIntervalMilliseconds(batchInterval)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .build();

        String messageBody = createRandomString();
        Client subscribeInBatchClient = new Client(eventHubConfiguration);
        TestSubscribeBatchCallback testSubscribeBatchCallback = new TestSubscribeBatchCallback( messageBody);
        subscribeInBatchClient.subscribe(testSubscribeBatchCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        buildAndSendMessages(subscribeInBatchClient, messageBody, batchSize);
        testSubscribeBatchCallback.block(batchSize);
        //batch size should equal number of messages retrieved via batch
        assertEquals(batchSize, testSubscribeBatchCallback.getMessageCount());
        subscribeInBatchClient.shutdown();
    }

    private boolean isTimeout(long startTime, long seconds) {
        return System.currentTimeMillis() - startTime > seconds;
    }


    @Test
    public void sendAcksOverMultipleThreads() throws EventHubClientException {
        int numberMessages = 5;
        EventHubConfiguration subscribeWithAckConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .acksEnabled(true)
                        .retryIntervalSeconds(DEFAULT_RETRY_INTERVAL)
                        .durationBeforeRetrySeconds(DEFAULT_DURATION_BEFORE_RETRY)
                        .subscriberName(subscriberName +"-1")
                        .subscriberInstance(subscriberID)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();

        String messageBody = createRandomString();
        String idPreface = createRandomString();
        final Client subscribeWithAckClient = new Client(subscribeWithAckConfiguration);

        TestSubscribeCallback testSubscribeCallback = new TestSubscribeCallback( messageBody);
        subscribeWithAckClient.subscribe(testSubscribeCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        // build and send messages
        buildAndSendMessages(client, messageBody, numberMessages, idPreface);
        // wait for messages
        testSubscribeCallback.block(numberMessages);
        pause(1000L);
        assertEquals(numberMessages, testSubscribeCallback.getMessageCount());
        List<Message> messages = testSubscribeCallback.getMessage();
        final List<Message> messageBuffer = new ArrayList<Message>();
        messageBuffer.addAll(messages);
        assertEquals(numberMessages, messageBuffer.size());
//remove message 1 from message buffer
        for (int i = 0; i < messageBuffer.size(); i++) {
            if (messageBuffer.get(i).getId().equals(idPreface + "-1")) {
                messageBuffer.remove(messageBuffer.get(i));
            }
        }

//We send acks for all messages except message 1 in different threads
        Thread thread = new Thread() {
            public void run() {
                try {
                    for (int i = 0; i < 2; i++) {
                        subscribeWithAckClient.sendAck(messageBuffer.get(i));
                    }
                } catch (EventHubClientException e) {
                    fail(e.getMessage());
                }
            }
        };
        thread.start();

        for (int i = 2; i < 4; i++) {
            subscribeWithAckClient.sendAck(messageBuffer.get(i));
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        messages.clear();
        testSubscribeCallback.resetCounts();

//wait for message 1 to be retried
        pause(DEFAULT_DURATION_BEFORE_RETRY * 1000L);
        testSubscribeCallback.block(1);
//should have reobtained message 1 since we didn't ack it
        assertEquals(1, testSubscribeCallback.getMessageCount());
        assertEquals(idPreface + "-1", testSubscribeCallback.getMessage().get(0).getId());
        subscribeWithAckClient.shutdown();
    }



    @Test
    @Ignore
    public void tooManySubscribers() throws EventHubClientException {
        int numSubClients = 10;
        int numMessages = 100;
        String messageBody = createRandomString();
        ArrayList<Client> subClients = new ArrayList<Client>();
        TestSubscribeCallback testSubscribeCallback = new TestSubscribeCallback(messageBody);

        for (int i = 0; i < numSubClients; i++) {
            EventHubConfiguration subscribeConfiguration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName + "-" + i).subscriberInstance(subscriberID)
                            .build())
                    .build();
            subClients.add(new Client(subscribeConfiguration));
            subClients.get(i).subscribe(testSubscribeCallback);
        }
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        buildAndSendMessages(client, messageBody, numMessages);
        testSubscribeCallback.block(numMessages * 5);
        assertEquals(5, testSubscribeCallback.getErrorCount());
        assertEquals(5 * numMessages, testSubscribeCallback.getMessageCount());

        for (Client c : subClients) {
            c.shutdown();
        }
    }

    @Test
    public void subscribeInBatchWithAck() throws EventHubClientException {
        String thisSubscriberName = "test-subscriber-subscribeInBatchWithAck";
        EventHubConfiguration subscribeInBatchWithPublish = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .batchingEnabled(true)
                        .subscriberName(thisSubscriberName + "-1")
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .batchSize(10)
                        .batchIntervalMilliseconds(1000)
                        .acksEnabled(true)
                        .durationBeforeRetrySeconds(20)
                        .retryIntervalSeconds(40)
                        .build())
                .publishConfiguration(new PublishConfiguration.Builder().build())
                .build();

        String messageBody = createRandomString();
        TestSubscribeBatchCallback batchCallback = new TestSubscribeBatchCallback(messageBody);
        Client batchClient = new Client(subscribeInBatchWithPublish);
        batchClient.subscribe(batchCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        //send 10 batches and a half
        buildAndSendMessages(batchClient, messageBody, 105);
        batchCallback.block(105);

        assertEquals(105, batchCallback.getMessageCount());

        batchClient.sendAcks(batchCallback.getMessage().subList(0, 100));
        batchCallback.resetCounts();

        batchCallback.block(5);
        assertEquals(5, batchCallback.getMessageCount());
        //We guarantee a maximum of the batch size in the batch interval
        //assertEquals(1, batchCallback.getBatchCount());
        //wait for the messages to be re-delivered
        batchCallback.resetCounts();
        batchCallback.block(5);
        assertEquals(5, batchCallback.getMessageCount());
        //We guarantee a maximum of the batch size in the batch interval
        //assertEquals(1, batchCallback.getBatchCount());
        batchClient.sendAcks(batchCallback.getMessage());
        batchClient.shutdown();
    }

    /**
     * Build a client with acks, publish 10 messages
     * Ack the 10 messages then shutdown
     * reopen with same subscriber name
     * @throws EventHubClientException
     */
    @Test
    public void testResubscribeAck() throws EventHubClientException {
        String thisSubscriberName = "test-subscriber-testResubscribeAck";
        String thisSubscriberID = "tester-testResubscribeAck";
        EventHubConfiguration config1 = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(
                        new SubscribeConfiguration.Builder()
                                .subscriberName(thisSubscriberName + "-1")
                                .subscriberInstance(thisSubscriberID)
                                .acksEnabled(true)
                                .durationBeforeRetrySeconds(60)
                                .retryIntervalSeconds(10)
                                .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                                .build())
                //.loggerConfiguraiton(new LoggerConfiguration.Builder().useJUL(true).prettyPrintJsonLogs(true).logLevel(LoggerConfiguration.LogLevel.ALL).build())
                .build();
        EventHubConfiguration config2 =  config1.cloneConfig()
                .subscribeConfiguration(config1.getSubscribeConfiguration()
                        .cloneConfig()
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.OLDEST)
                        .build()
                ).build();
        Client client= new Client(config1);
        String message = createRandomString();
        TestSubscribeAckCallback subCallback = new TestSubscribeAckCallback(message, client);
        client.subscribe(subCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        buildAndSendMessages(client, message, 10, "first");
        subCallback.block(10);
        assertEquals(10, subCallback.getMessageCount());
        pause(30000L);
        client.shutdown();
        pause(5000L);
        client = new Client(config2);
        subCallback = new TestSubscribeAckCallback(message, client);
        client.subscribe(subCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        buildAndSendMessages(client, message, 10, "second");
        //wait for 20 but make sure that we only get 10 back
        subCallback.block(20);
        assertEquals(10, subCallback.getMessageCount());
        client.shutdown();
    }

    /**
     * This set assures that the batch subscription rate is limited to the batch size per batch interval
     * @throws EventHubClientException
     */
    @Ignore
    @Test
    public void subscribeBatchRate() throws EventHubClientException {
        EventHubConfiguration config = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().batchingEnabled(true).batchSize(10).batchIntervalMilliseconds(1000).build())
                .build();

        Client subscribeBatchClient= new Client(config);
        String message = createRandomString();
        TestSubscribeBatchCallback subscribeBatchRateCallback = new TestSubscribeBatchCallback(message);
        subscribeBatchClient.subscribe(subscribeBatchRateCallback);
        subscribeBatchRateCallback.resetCounts();
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        List<Ack> theAcks = buildAndSendMessages(subscribeBatchClient, message, 100);

        //Acks for 100 messages published
        assertEquals(100, theAcks.size());

        //First set of messages to be received in ~ 1 second
        pause(900L);
        assertTrue(subscribeBatchRateCallback.getMessageCount() <= 10);
        subscribeBatchRateCallback.resetCounts();
        pause(100L);

        //Second set of messages to be received in ~ 1 second
        pause(900L);
        assertTrue(subscribeBatchRateCallback.getMessageCount() <= 10);
        subscribeBatchRateCallback.resetCounts();
        pause(100L);

        //Rest of messages to be received in remaining 8 seconds
        pause(8000L);
        assertTrue(subscribeBatchRateCallback.getMessageCount() <= 80);

        subscribeBatchClient.shutdown();
    }

    @After
    public void shutdown() {
        client.shutdown();
    }
}
