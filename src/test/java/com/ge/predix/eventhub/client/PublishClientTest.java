package com.ge.predix.eventhub.client;

import static com.ge.predix.eventhub.client.utils.TestUtils.SUBSCRIBER_ACTIVE_WAIT_LENGTH;
import static com.ge.predix.eventhub.client.utils.TestUtils.createRandomString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;

import com.ge.predix.eventhub.Ack;
import com.ge.predix.eventhub.AckStatus;
import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.Messages;
import com.ge.predix.eventhub.client.utils.TestUtils;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.ge.predix.eventhub.test.categories.SanityTest;
import com.google.protobuf.ByteString;

/**
 * Created by 212571077 on 7/8/16.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class PublishClientTest {

    final String subscriberName = "test-subscriber";
    final String subscriberID = "tester";

    static Client syncClient;
    static Client asyncClient;

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

    @Before
    public void createClient() {
// make the async and sync clients
        try {
            EventHubConfiguration asyncConfiguration = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().acksEnabled(true).build())
                    .build();
            asyncClient  = new Client(asyncConfiguration);
            EventHubConfiguration syncConfiguration = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName).subscriberInstance(subscriberID).build())
                    .build();
            syncClient  = new Client(syncConfiguration);
        } catch (EventHubClientException.InvalidConfigurationException e) {
            System.out.println("*** Could not make client ***");
        }
    }

    @Test
    @Category(SanityTest.class)
    public void addMessage() throws EventHubClientException {
        for (int j = 0; j < 100; j++) {
            syncClient.addMessage(j+"", "message", null);
        }
        assertEquals(100, syncClient.publishClient.messages.getMsgCount());
    }

    @Test
    @Category(SanityTest.class)
    public void addTooManyMessages() throws EventHubClientException {
        int maxMessages = 11; // keep odd
        EventHubConfiguration syncConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).maxAddedMessages(maxMessages).build())
                .build();
        Client limitedClient  = new Client(syncConfiguration);

        boolean caught = false;
        try {
            for (int j = 0; j < Math.floor(maxMessages/2) + 1; j++) {
                limitedClient.addMessage(j + "", "message", null).addMessage(j + "", "message", null);
            }
        } catch (EventHubClientException.AddMessageException e) {
            caught = true;
        }

        if(!caught) {
            fail();
        }
    }

    @Test
// test all three addMessage methods
    @Category(SanityTest.class)
    public void addMessageE2E() throws EventHubClientException {
        EventHubConfiguration sampleConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .build();

// create message content
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("key", "value");
        String id = "id";
        String messageContent = "message";
        String zoneID = sampleConfiguration.getZoneID();

        Message message = createMessage(tags, id, messageContent, zoneID);
        Messages messages = Messages.newBuilder()
                .addMsg(message)
                .build();

// clear queue for subscriber
        TestUtils.TestSubscribeCallback callback = new TestUtils.TestSubscribeCallback( messageContent);
        syncClient.subscribe(callback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        callback.resetCounts();

// deliver all three messages

        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
        List<Ack> acks = null;
        try{
            acks = syncClient
                    .addMessage(id, messageContent, tags)
                    .addMessage(message)
                    .addMessages(messages)
                    .flush();
        }catch (EventHubClientException.SyncPublisherFlushException e){
            syncPublisherFlushException = e;
            for(Throwable t : e.getThrowables()){
                t.printStackTrace();
            }
        }catch (EventHubClientException e){
            throw e;
        }finally {
            assertNull(syncPublisherFlushException);
        }

        assertEquals(3, acks.size());
        assertEquals(AckStatus.ACCEPTED, acks.get(0).getStatusCode());
        assertEquals(AckStatus.ACCEPTED, acks.get(1).getStatusCode());
        assertEquals(AckStatus.ACCEPTED, acks.get(2).getStatusCode());

// wait for messages
        callback.block(3);

// subscribe to ensure messages content was preserved
        List<Message> receivedMessages = callback.getMessage();
        assertEquals(3, receivedMessages.size());
        for (Message msg : receivedMessages) {
            assertEquals(message.getId(), msg.getId());
            assertEquals(message.getBody(), msg.getBody());
            assertEquals(message.getTags(), msg.getTags());
        }
    }

    /*
    Setup: Send Messages with different tags.
    Expected: Subscriber retrieves messages with different tags
    */
    @Test
    @Category(SanityTest.class)
    public void sendMessagesWithTags() throws EventHubClientException {
        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .build();

// create tags
        Map<String, String> tag1 = new HashMap<String, String>();
        Map<String, String> tag2 = new HashMap<String, String>();
        tag1.put("tag1", "tag1");
        tag2.put("tag2", "tag2");

//generic message fields
        String id = "id";
        String messageContent = "message";

// clear queue for subscriber
        TestUtils.TestSubscribeCallback callback = new TestUtils.TestSubscribeCallback( messageContent);
        syncClient.subscribe(callback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        callback.resetCounts();

// deliver two messages with differing tags
        List<Ack> acks = syncClient
                .addMessage(id, messageContent, tag1)
                .flush();    // deliver two messages with differing tags

        pause(1000L);// pause for 1 seconds for first message to send

        acks.addAll(syncClient
                .addMessage(id, messageContent, tag2)
                .flush());

        callback.block(2);// pause for 2 seconds for messages to be retrieved

        List<Message> receivedMessages = callback.getMessage();

// ensure both messages retrieved
        assertEquals(2, receivedMessages.size());

        Message firstMessage = receivedMessages.get(0);
        Message secondMessage = receivedMessages.get(1);

//Assert different tags are stored with appropriate message
        assertEquals(tag1, firstMessage.getTags());
        assertEquals(tag2, secondMessage.getTags());
    }

    @Test
    @Category(SanityTest.class)
// test async and sync publish by sending 100 messages 10 times
    public void publish() throws EventHubClientException{
// make async callback
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient.registerPublishCallback(callback);

// send messages
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                syncClient.addMessage(i+"-"+j, "message", null);
                asyncClient.addMessage(i+"-"+j, "message", null);
            }
            asyncClient.flush();
            List<Ack> acks = syncClient.flush();

// check sync
            assertEquals(100, acks.size());
            assertEquals(AckStatus.ACCEPTED, acks.get(0).getStatusCode());
        }

        callback.block(1000);
        assertEquals(1000, callback.getAckCount());
    }


    @Test
    @Category(SanityTest.class)
    public void multipleAsyncClientPublish() throws EventHubClientException {
        EventHubConfiguration asyncConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().build())

                .build();
        Client asyncClient2  = new Client(asyncConfiguration);
        Client asyncClient3  = new Client(asyncConfiguration);

        // share same callback
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient.registerPublishCallback(callback);
        asyncClient2.registerPublishCallback(callback);
        asyncClient3.registerPublishCallback(callback);

        // send messages
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                asyncClient.addMessage("1"+i+"-"+j, "message", null);
                asyncClient2.addMessage("2"+i+"-"+j, "message", null);
                asyncClient3.addMessage("3"+i+"-"+j, "message", null);
            }
            asyncClient.flush();
            asyncClient2.flush();
            asyncClient3.flush();
        }

        // check async
        pause(5000L); // wait for acks to arrive
        callback.block(10 * 100 * 3);
        TestUtils.PublishCallback publishCallback = (TestUtils.PublishCallback) asyncClient.getPublishCallback();
        assertEquals(10 * 100 * 3, publishCallback.getAckCount());
        asyncClient2.shutdown();
        asyncClient3.shutdown();
    }

    @Test
    @Category(SanityTest.class)
    public void multipleSyncClientPublish() throws EventHubClientException {
        EventHubConfiguration syncConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .build();
        Client syncClient2  = new Client(syncConfiguration);
        Client syncClient3  = new Client(syncConfiguration);

        int messageCount = 100;
        // send messages
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < messageCount; j++) {
                syncClient.addMessage("1"+i+"-"+j, "message", null);
                syncClient2.addMessage("2"+i+"-"+j, "message", null);
                syncClient3.addMessage("3"+i+"-"+j, "message", null);
            }

            assertEquals(messageCount, syncClient.publishClient.messages.getMsgCount());
            assertEquals(messageCount, syncClient2.publishClient.messages.getMsgCount());
            assertEquals(messageCount, syncClient3.publishClient.messages.getMsgCount());

            List<Ack> acks = syncClient.flush();
            List<Ack> acks2 = syncClient2.flush();
            List<Ack> acks3 = syncClient3.flush();

            // check sync
            //System.out.println("Checking for iteration: " + (i+1));
            assertEquals(messageCount, acks.size());
            assertEquals(messageCount, acks2.size());
            assertEquals(messageCount, acks3.size());
            assertEquals(AckStatus.ACCEPTED, acks.get(0).getStatusCode());
            assertEquals(AckStatus.ACCEPTED, acks2.get(0).getStatusCode());
            assertEquals(AckStatus.ACCEPTED, acks3.get(0).getStatusCode());

        }

        syncClient2.shutdown();
        syncClient3.shutdown();
    }

    @Test
    @Category(SanityTest.class)
    // test adding messages with two threads that add messages
    public void withThreadsAddMessage() throws InterruptedException, EventHubClientException {
        // make async callback
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient.registerPublishCallback(callback);

        // make threads
        final CountDownLatch startSync = new CountDownLatch(1);
        Thread addThread1 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    for (int i=0; i < 100; i++) {
                        syncClient.addMessage("1-"+i, "addThread1", null);
                        asyncClient.addMessage("1-"+i, "addThread1", null);
                    }
                } catch (EventHubClientException e) {
                    fail();
                }
            }
        };
        Thread addThread2 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    for (int i=0; i < 101; i++) {
                        syncClient.addMessage("2-"+i, "addThread2", null);
                        asyncClient.addMessage("2-"+i, "addThread2", null);
                    }
                } catch (EventHubClientException e) {
                    fail();
                }
            }
        };

        // start sending in two threads
        addThread1.start();
        addThread2.start();
        startSync.countDown();
        addThread1.join();
        addThread2.join();

        // make sure messages are added
        assertEquals(201, asyncClient.publishClient.messages.getMsgCount());
        assertEquals(201, syncClient.publishClient.messages.getMsgCount());

        // send the messages
        asyncClient.flush();
        List<Ack> acks = syncClient.flush();

        // check sync
        assertEquals(201, acks.size());
        assertEquals(AckStatus.ACCEPTED, acks.get(0).getStatusCode());

        callback.block(201);

        // check async
        assertEquals(201, callback.getAckCount());
    }

    @Test
    @Category(SanityTest.class)
    // two thread sends, one adds
    public void withThreadsPublishAndAdd() throws InterruptedException, EventHubClientException {
        // make sync counter
        final AtomicInteger syncCounter = new AtomicInteger(0);
        // make async callback
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient.registerPublishCallback(callback);

        // make threads
        final CountDownLatch startSync = new CountDownLatch(1);
        final CountDownLatch stopSync = new CountDownLatch(2); // 1 for syncThread and 1 for addThread
        Thread flushThread1 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                while (stopSync.getCount() < 0) {
                    List<Ack> acks = null;
                    try {
                        asyncClient.flush();
                        acks = syncClient.flush();
                    } catch (EventHubClientException e) {
                        e.printStackTrace();
                    }
                    syncCounter.addAndGet(acks.size());
                }
            }
        };
        Thread flushThread2 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                while (stopSync.getCount() < 0) {
                    List<Ack> acks = null;
                    try {
                        asyncClient.flush();
                        acks = syncClient.flush();
                    } catch (EventHubClientException e) {
                        e.printStackTrace();
                    }
                    syncCounter.addAndGet(acks.size());
                }
            }
        };
        Thread sendThread = new Thread() {
            public void run() {
                Message singleMessage = Message.newBuilder().setId("11").setZoneId(syncClient.configuration.getZoneID())
                        .setBody(ByteString.copyFromUtf8("other message")).build();

                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    List<Ack> acks;
                    while (stopSync.getCount() < 0) {
                        syncClient.addMessage(singleMessage).flush();
                        asyncClient.addMessage(singleMessage).flush();

                        if (stopSync.getCount() > 1) { // ensure we go through this cycle at least once
                            stopSync.countDown();
                        }
                        pause(50L);
                    }
                } catch (EventHubClientException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread addThread = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    for (int i = 0; i < 500; i++) {
                        syncClient.addMessage("2-" + i, "addThread2", null);
                        asyncClient.addMessage("2-" + i, "addThread2", null);
                    }
                } catch (EventHubClientException e) {
                    fail();
                }
                stopSync.countDown();
            }
        };

        addThread.start();
        flushThread1.start();
        flushThread2.start();
        sendThread.start();
        startSync.countDown();
        stopSync.await(2, TimeUnit.SECONDS);
        addThread.join();
        flushThread1.join();
        flushThread2.join();
        sendThread.join();


        // send any remaining messages in the queues
        List<Ack> acks = syncClient.flush();
        syncCounter.addAndGet(acks.size());
        asyncClient.flush();

        // check sync
        assertEquals(500, syncCounter.get());

        // check async
        callback.block(500);
        assertEquals(500, callback.getAckCount());
    }

    @Test
    @Category(SanityTest.class)
    // test that we only get Nacks, Acks/Nacks can be false but this should override it
    public void asyncNacksOnly() throws EventHubClientException {
        // make client that will only get nacks
        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC)
                        .ackType(PublishConfiguration.AcknowledgementOptions.NACKS_ONLY)
                        .build())
                .build();
        Client asyncClient2 = new Client(configuration);

        // make async callback
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient2.registerPublishCallback(callback);

        // send message
        asyncClient2.addMessage("1", "I make an ack", null);
        Message badMessage = Message.newBuilder().setBody(ByteString.copyFromUtf8("I should make an nack because I do not have an id")).build();
        asyncClient2.addMessage(badMessage);
        asyncClient2.flush();

        // check response
        callback.block(1);
        assertEquals(1, callback.getAckCount());

        asyncClient2.shutdown();
    }

    //@Test // I don't work yet because there needs to be a service fix to not send nacks when parts of the payload is missing (like id)
    // test that we get no acks or nacks
    public void asyncNoAcksNoNacks() throws EventHubClientException {
    // make client that will only get nacks
        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC)
                        .ackType(PublishConfiguration.AcknowledgementOptions.NONE)
                        .build())
                .build();
        Client asyncClient2 = new Client(configuration);

        // make async callback
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient2.registerPublishCallback(callback);

        // send message
        asyncClient2.addMessage("good message", "I make an ack", null);
        Message badMessage = Message.newBuilder().setBody(ByteString.copyFromUtf8("I should make an nack because I do not have an id")).build();
        asyncClient2.addMessage(badMessage);
        asyncClient2.flush();

        // check response
        callback.block(1);
        assertEquals(0, callback.getAckCount());

        asyncClient2.shutdown();
    }

    //@Test cacheAcksAndNacks currently is not ready on the server side
    // test that will not cache acks
    public void asyncNoCache() throws EventHubClientException, InterruptedException {
        // make client will not cache the acks
        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
        //       .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC)().cacheAcksAndNacks(false).build())
                .build();
        Client asyncClientNoCache = new Client(configuration);
        final TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClientNoCache.registerPublishCallback(callback);
        asyncClient.registerPublishCallback(callback);

        // Thread to wait for ack
        final CountDownLatch waitNoCache = new CountDownLatch(1);
        Thread waitNoCacheThread = new Thread() {
            public void run() {
                while(waitNoCache.getCount() > 0) {
                    if(callback.getAckCount() > 0) {
                        callback.resetCounts();
                        waitNoCache.countDown();
                    }
                }
            }
        };

        // send message with no cache
        asyncClientNoCache.addMessage("1", "I will be sent and acks will come back faster than 500ms", null);
        asyncClientNoCache.flush();
        Long startTime = System.currentTimeMillis();
        waitNoCacheThread.start();
        waitNoCache.await(1, TimeUnit.SECONDS);
        waitNoCache.countDown();
        waitNoCacheThread.join();
        Long timeDifference = System.currentTimeMillis() - startTime;
        System.out.println(timeDifference);
        assertTrue(timeDifference < 750); // give some buffer .. usually ~400ms

        // Thread to wait for ack
        final CountDownLatch waitCache = new CountDownLatch(1);
        Thread waitCacheThread = new Thread() {
            public void run() {
                while(waitCache.getCount() > 0) {
                    if(callback.getAckCount() > 0) {
                        callback.resetCounts();
                        waitCache.countDown();
                    }
                }
            }
        };

        // send message with cache
        asyncClient.addMessage("1", "I will be sent and acks will come back slower than 500ms", null);
        asyncClient.flush();
        startTime = System.currentTimeMillis();
        waitCacheThread.start();
        waitCache.await(1, TimeUnit.SECONDS);
        waitCache.countDown();
        waitCacheThread.join();
        timeDifference = System.currentTimeMillis() - startTime;
        System.out.println(timeDifference);
        assertTrue(timeDifference > 500);

        asyncClientNoCache.shutdown();
    }

    @Test
    @Category(SanityTest.class)
    public void asyncCacheInterval() throws EventHubClientException, InterruptedException {
        // test with 100ms interval
        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).cacheAckIntervalMillis(100).build())
                .build();
        final Client asyncCache100Client = new Client(configuration);
        final TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncCache100Client.registerPublishCallback(callback);
        final int totalMessagesToSend = 10;

        final CountDownLatch stopCache100 = new CountDownLatch(1); // signal that the messages are done sending
        final AtomicLong totalTime100 = new AtomicLong(0); // capture total time taken so time per message can be calculated
        final AtomicInteger runCount100 = new AtomicInteger(0); // how many messages to test

        //Send 10 messages waiting for the ack each time and keeping track of total time waiting for ack
        Thread cache100 = new Thread() {
            public void run() {
                try {
                    for (; runCount100.get() < totalMessagesToSend; runCount100.incrementAndGet()) {
                        asyncCache100Client.addMessage("id", "Ack returned around 100ms for me", null);
                        Long startTime = System.currentTimeMillis();
                        asyncCache100Client.flush();
                        callback.block(1);
                        assertEquals(1, callback.getAckCount());
                        long timeTillAck = System.currentTimeMillis() - startTime;
                        totalTime100.addAndGet(timeTillAck); // add the time we waited for the ack
                        callback.resetCounts(); // reset callback's ack count
                    }
                    stopCache100.countDown(); // done sending
                } catch (EventHubClientException e) {
                    fail();
                }
            }
        };

        startPublishStream(asyncCache100Client, callback);
        cache100.start();
        stopCache100.await(5, TimeUnit.SECONDS);
        cache100.join();

        long averageAckTime = totalTime100.get() / totalMessagesToSend;
        assertTrue(averageAckTime < 500);
        asyncCache100Client.shutdown();
    }

    private void startPublishStream(Client asyncCache100Client, TestUtils.PublishCallback callback) throws EventHubClientException {
        asyncCache100Client.addMessage("id", "body", null).flush();
        pause(2000L);
        callback.resetCounts();
    }

    @Test
    @Category(SanityTest.class)
        // test that the override of sync timeout
    public void syncTimeoutOverride() throws EventHubClientException {
        // make client with a override sync timeout which is too short to receive messages

        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).timeout(0).build())
                .build();
        Client syncClient2 = new Client(configuration);

        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
        // send message
        try{
            syncClient2.addMessage("1", "I will be sent BUT acks will come too late", null).flush();
        }catch (EventHubClientException.SyncPublisherFlushException e){
            syncPublisherFlushException = e;
        }finally {
            assertNotNull(syncPublisherFlushException);
            assertEquals(1, syncPublisherFlushException.getThrowables().size());
            assertTrue(syncPublisherFlushException.getThrowables().get(0) instanceof EventHubClientException.SyncPublisherTimeoutException);
        }
        syncClient2.shutdown();
    }

    @Test
    @Category(SanityTest.class)
    public void closeConnectionOnError() throws EventHubClientException {
        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();

        EventHubConfiguration config = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .automaticTokenRenew(false)
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .build();

        Client asyncClient2 = new Client(config);
        asyncClient2.registerPublishCallback(callback);
        asyncClient2.flush();   // will trigger connection and callback with "Missing Authorization header"

        // wait for error
        pause(1500L);

        assertThat(callback.getErrorCount(), greaterThanOrEqualTo(1));
        callback.resetCounts();

        asyncClient2.setAuthToken("bananas!"); // will callback with "Could not authenticate user"
        asyncClient2.publishClient.backOffDelay.attempt.set(0); // force retry back to 0 so timeouts aren't as long
        asyncClient2.addMessage("-1", "I will be lost!", null).flush();

        pause(1000L); // wait for stream to close after trying max retries from BackOffDelay

        asyncClient2.addMessage("0", "I should fail for being sent on closed stream! (I will be deleted from queue because it is up to user to check the callbacks)", null).flush(); // will callback with "Stream is not open"
        assertTrue(asyncClient2.publishClient.isStreamClosed());

        // get token from syncClient client
        syncClient.addMessage("id","dummy message",null).flush();
        asyncClient2.setAuthToken(syncClient.getAuthToken());

        asyncClient2.addMessage("1", "I should pass!", null).flush();
        asyncClient2.addMessage("2", "I should pass!", null).flush();
        callback.block(2);// wait for acks

        assertEquals(2, callback.getAckCount());
        assertThat(callback.getErrorCount(), greaterThanOrEqualTo(2));

        assertTrue(!asyncClient2.publishClient.isStreamClosed());

        asyncClient2.shutdown();
    }

    @Test
    @Category(SanityTest.class)
    public void testPubSyncErrors() throws EventHubClientException {

        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
        try {
            List<Ack> acks = syncClient.addMessage("id", "body", null).flush();
            assertEquals(1, acks.size());
            syncClient.setAuthToken("badToken");
            syncClient.addMessage("id2", "body2", null).flush();
        } catch (EventHubClientException.SyncPublisherFlushException e) {
            syncPublisherFlushException = e;
            pause(1000L);
        }finally {
            assertNotNull(syncPublisherFlushException);
        }
    }

    @Test
    @Category(SanityTest.class)
    public void testEmptyBody() throws EventHubClientException {
        List<Ack> acks = syncClient.addMessage("id", "", null).flush();
        System.out.println(acks);
    }

    private Message createMessage(Map<String, String> tag1, String id, String messageContent, String zoneID) {
        return Message.newBuilder().setId(id).setBody(ByteString.copyFromUtf8(messageContent)).putAllTags(tag1).setZoneId(zoneID).build();
    }


    private void pause(Long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            System.out.println("~~~~~ TIMEOUT INTERRUPTED ~~~~~");
        }
    }

    /**
     * Test when a message is too large that the service returns a bad request for both async and sync
     * @throws EventHubClientException if something goes wrong
     */
    @Test
    @Category(SanityTest.class)
    public void addTooLargeMessage() throws EventHubClientException {
        EventHubConfiguration sampleConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .build();

// create message content
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("key", "value");
        String id = "id";
        String messageContentLarge = createRandomString(1024 * 1024 * 2);
        String zoneID = sampleConfiguration.getZoneID();

        Message messageLarge = createMessage(tags, "id-large", messageContentLarge, zoneID);

        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;

        TestUtils.PublishCallback callback = new TestUtils.PublishCallback();
        asyncClient.registerPublishCallback(callback);
        List<Ack> acks = null;
        try{
            acks = syncClient.addMessage(messageLarge).flush();
        }catch (EventHubClientException.SyncPublisherFlushException e){
            syncPublisherFlushException = e;
            for(Throwable t : e.getThrowables()){
                t.printStackTrace();
            }
        }catch (EventHubClientException e){
            throw e;
        }finally {
            assertNull(syncPublisherFlushException);
        }


        assertEquals(1, acks.size());
        assertEquals(AckStatus.BAD_REQUEST, acks.get(0).getStatusCode());

        callback.resetCounts();
        asyncClient.addMessage(messageLarge).flush();
        callback.block(1);
        assertEquals(1, callback.getAckCount());
        assertEquals(AckStatus.BAD_REQUEST, callback.getAcks().get(0).getStatusCode());
    }

    @After
    public void shutdown() {
        syncClient.shutdown();
        asyncClient.shutdown();
    }

}
