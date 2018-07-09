package com.ge.predix.eventhub.client;

import static com.ge.predix.eventhub.client.utils.TestUtils.buildAndSendMessages;
import static com.ge.predix.eventhub.client.utils.TestUtils.createRandomString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubUtils;
import com.ge.predix.eventhub.client.utils.TestUtils;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.ge.predix.eventhub.stub.Ack;
import com.ge.predix.eventhub.stub.AckStatus;
import com.ge.predix.eventhub.stub.Message;

/**
 * Created by williamgowell on 10/3/17.
 */

@Ignore
public class PerformanceTests {
    static String subscriberName = "performance-test-subscriber";
    static int maxMessageSizeBytes = 1024 * 1024;
    static int timeout = 10;
    private class PublishCallback implements Client.PublishCallback {
        CountDownLatch finishLatch = new CountDownLatch(0);
        AtomicInteger receivedAcks = new AtomicInteger(0);
        AtomicLong firstAckTime = new AtomicLong(0);

        @Override
        public synchronized void onAck(List<Ack> acks) {
            if (firstAckTime.get() == 0) {
                firstAckTime.set(System.currentTimeMillis());
            }
            receivedAcks.addAndGet(acks.size());
            for (int i = 0; i < acks.size(); i++) {
                if(!acks.get(i).getStatusCode().equals(AckStatus.ACCEPTED)){
                    System.out.println("got a non accepted code in a pub ack: " + acks.get(i));
                }
                if (finishLatch.getCount() != 0) {
                    finishLatch.countDown();
                }
            }


        }

        void block(int count) throws InterruptedException {
            System.out.println("Starting pub block, countdownLatch " + count + " currentReceived " + receivedAcks.get());
            finishLatch = new CountDownLatch(count - receivedAcks.get());

            try {

                for(int i =0;i<timeout && finishLatch.getCount() != 0;i++){
                    finishLatch.await(1 , TimeUnit.MINUTES);
                    System.out.println("pub block has " + finishLatch.getCount() +" left in the latch");
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (finishLatch.getCount() != 0) {
                System.out.println("pub block finished with " + finishLatch.getCount() + " left in the latch");
            }
            System.out.print("finished pub block");
        }

        synchronized void resetCounts() {
            this.firstAckTime.set(0);
            this.receivedAcks.set(0);
        }

        @Override
        public void onFailure(Throwable throwable) {

        }
    }

    private class MasterSubscribeCallback {
        CountDownLatch finishLatch = new CountDownLatch(0);
        AtomicInteger receivedMessages = new AtomicInteger(0);
        AtomicLong firstReceiveTime = new AtomicLong(0);

        MasterSubscribeCallback() {

        }

        class BatchSubscribeCallback implements Client.SubscribeBatchCallback {
            Client c;
            MasterSubscribeCallback mc;

            BatchSubscribeCallback(MasterSubscribeCallback mc) {
                this.mc = mc;
                this.c = null;
            }

            BatchSubscribeCallback(MasterSubscribeCallback mc, Client c) {
                this.c = c;
                this.mc = mc;
            }

            @Override
            public void onMessage(List<Message> messages) {
                if (c != null) {
                    try {
                        c.sendAcks(messages);
                    } catch (EventHubClientException e) {
                        e.printStackTrace();
                    }
                }
                mc.countDown(messages.size());

            }

            @Override
            public void onFailure(Throwable throwable) {

            }
        }

        class SubscribeCallback implements Client.SubscribeCallback {
            Client c;
            MasterSubscribeCallback mc;

            SubscribeCallback(MasterSubscribeCallback mc) {
                this.mc = mc;
                this.c = null;
            }

            SubscribeCallback(MasterSubscribeCallback mc, Client c) {
                this.mc = mc;
                this.c = c;
            }

            @Override
            public void onMessage(Message m) {
                if (c != null) {
                    try {
                        c.sendAck(m);
                    } catch (EventHubClientException e) {
                        e.printStackTrace();
                    }
                }
                mc.countDown(1);
            }

            @Override
            public void onFailure(Throwable throwable) {

            }
        }

        void addClient(boolean batchingEnabled, boolean acksEnabled, Client c) throws EventHubClientException {
            if (batchingEnabled) {

                if (acksEnabled) {
                    c.subscribe(new BatchSubscribeCallback(this, c));
                } else {
                    c.subscribe(new BatchSubscribeCallback(this));
                }
            } else {
                if (acksEnabled) {
                    c.subscribe(new SubscribeCallback(this, c));
                } else {
                    c.subscribe(new SubscribeCallback(this));
                }
            }
        }

        void block(int count) throws InterruptedException {
            System.out.println(String.format("starting sub block %d", count));
            if (receivedMessages.get() > count) {
                return;
            }
            finishLatch = new CountDownLatch(count - receivedMessages.get());

            for(int i =0;i<timeout && finishLatch.getCount() != 0;i++){
                finishLatch.await(1 , TimeUnit.MINUTES);
                System.out.println("sub block has left: " + finishLatch.getCount());
            }

            finishLatch.await(1 , TimeUnit.MINUTES);
            finishLatch.await(1 , TimeUnit.MINUTES);

            if (finishLatch.getCount() != 0)
                System.out.println("Missed Messages on sub block, has left: " + finishLatch.getCount());
        }

        void countDown(int count) {
            receivedMessages.addAndGet(count);
            firstReceiveTime.set(firstReceiveTime.get() == 0 ? System.currentTimeMillis() : firstReceiveTime.get());
            for (int i = 0; i < count; i++)
                if (finishLatch.getCount() > 0)
                    finishLatch.countDown();


        }

        void resetCount() {
            receivedMessages.set(0);
            firstReceiveTime.set(0);
        }
    }

    static int[] batchSizes = new int[]{0, 10, 100, 1000, 10000};
    static int[] messageCounts = new int[]{0, 10, 100, 1000, 10000};
    static int[] messageSizesBytes = new int[]{0, 10, 100, 1000, 10000};
    static int[] subscribeCounts = new int[]{0, 10, 100, 1000, 10000};
    boolean[] trueFalse = new boolean[]{true, false};
    static ArrayList<String> testResults;
    static StringBuilder currentTestResults;

    static PublishConfiguration.PublisherType[] pubtype = new PublishConfiguration.PublisherType[]{PublishConfiguration.PublisherType.ASYNC, PublishConfiguration.PublisherType.SYNC};

    static EventHubConfiguration baseBuilder;
    @BeforeClass
    public static void init(){

        try {
            baseBuilder = new EventHubConfiguration.Builder()
                    .host(System.getenv("EVENTHUB_URI"))
                    .port(Integer.parseInt(System.getenv("EVENTHUB_PORT")))
                    .zoneID(System.getenv("ZONE_ID"))
                    .clientID(System.getenv("CLIENT_ID"))
                    .clientSecret(System.getenv("CLIENT_SECRET"))
                    .authURL(System.getenv("AUTH_URL"))
                    .build();
        } catch (EventHubClientException.InvalidConfigurationException e) {
            e.printStackTrace();
        }
        testResults = new ArrayList<>();
        testResults.add("baseBuilder");
        testResults.add(baseBuilder.toString());
    }

    @AfterClass
    public static void printResults(){
        System.out.println(EventHubUtils.formatJson(testResults.toArray(new String[0])));
    }

    @Before
    public void startStringBuilder(){
        currentTestResults = new StringBuilder("[");
    }

    private static void addTestResult(String testName){
        testResults.add(testName);
        if(currentTestResults.length() ==1){
            testResults.add("[]");
        }else{
            testResults.add(currentTestResults.deleteCharAt(currentTestResults.length() - 1).append("]").toString());
        }
    }

    /**
     * Test variations in message size and counts
     *
     * @throws EventHubClientException if something bad happens
     * @throws InterruptedException if something bad happens
     */
    @Test
    public void testMessageVariation() throws EventHubClientException, InterruptedException {
        for (int currentMessageCount : messageCounts) {
            for (int currentMessageSize : messageSizesBytes) {
                currentTestResults.append(runTests(baseBuilder, currentMessageCount, currentMessageSize, 1, 0, false, PublishConfiguration.PublisherType.ASYNC)).append(',');
            }
        }
        addTestResult("message_variation");
    }

    /**
     * Test the performance difference with different subscribers
     *
     * @throws EventHubClientException if something bad happens
     * @throws InterruptedException if something bad happens
     */
    @Test
    public void testMultipleSubscribers() throws EventHubClientException, InterruptedException {
        for (int currentMessageCount : messageCounts) {
            for (int currentSubscriberCount : subscribeCounts) {
                currentTestResults.append(runTests( baseBuilder, currentMessageCount, 1000, currentSubscriberCount, 0, false, PublishConfiguration.PublisherType.ASYNC)).append(',');
            }
        }
        addTestResult("multiple_subscriber");
    }

    /**
     * Test the difference in async and sync performance tests
     *
     * @throws EventHubClientException
     * @throws InterruptedException
     */
    @Test
    public void testPublisherType() throws EventHubClientException, InterruptedException {
        //Test the difference between Aysnc and Sync
        for (int currentMessageCount : messageCounts) {
            for (PublishConfiguration.PublisherType publisherType : pubtype) {
                currentTestResults.append(runTests(baseBuilder, currentMessageCount, 1000, 5, 0, false, publisherType)).append(',');
            }
        }
        addTestResult("publisher_type");
    }

    /**
     * See how enabling acks effects performance
     *
     * @throws EventHubClientException
     * @throws InterruptedException
     */
    @Test
    public void testAck() throws EventHubClientException, InterruptedException {
        //test if acks have any effect
        for (int currentMessageCount : messageCounts) {
            currentTestResults.append(runTests(  baseBuilder, currentMessageCount, 100, 1, 0, true, PublishConfiguration.PublisherType.ASYNC)).append(',');
        }
        addTestResult("subscriber_ack");
    }

    /**
     * See how enabling batching effects performance
     * @throws EventHubClientException
     * @throws InterruptedException
     */
    @Test
    public void testBatching() throws EventHubClientException, InterruptedException {
        //test batching
        for (int currentMessageCount : messageCounts) {
            for (int batchSize : batchSizes) {
                currentTestResults.append(runTests(baseBuilder, currentMessageCount, 100, 1, batchSize, false, PublishConfiguration.PublisherType.ASYNC)).append(',');
            }
        }
        addTestResult("subscriber_batching");
    }

    @Test
    @Ignore
    public void customTest() throws EventHubClientException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        //builder.append(runTests(baseBuilder, 1000000, 100, 1, 0, false,  PublishConfiguration.PublisherType.ASYNC));
        //builder.append(runTests(baseBuilder, 1000000, 100, 1, 0, false,  PublishConfiguration.PublisherType.SYNC));
        //builder.append(runTests(baseBuilder, 1000000, 1000, 1, 0, false,  PublishConfiguration.PublisherType.ASYNC));
        //builder.append(runTests(baseBuilder, 1000000, 1000, 1, 0, false,  PublishConfiguration.PublisherType.SYNC));
        for(int i=0;i<5;i++){
                builder.append(runTests(baseBuilder, 10_000_000, 10, 1, 10000, false,  PublishConfiguration.PublisherType.ASYNC));
               // builder.append(runTests(baseBuilder, 10_000_000, 10, 1, 10000, true,  PublishConfiguration.PublisherType.ASYNC));
        }
        pause(100000L);

        //builder.append(runTests(baseBuilder, 1000000, 100, 1, 1000, false,  PublishConfiguration.PublisherType.ASYNC));
        //builder.append(runTests(baseBuilder, 1000000, 100, 1, 10000, false,  PublishConfiguration.PublisherType.ASYNC));

        System.out.println(builder.toString());
/*
        builder.append(runTests(baseBuilder, 10000000, 1000, 1, 0, false,  PublishConfiguration.PublisherType.ASYNC));
        builder.append(runTests(baseBuilder, 10000000, 1000, 1, 0, true,  PublishConfiguration.PublisherType.ASYNC));




        builder.append(runTests(baseBuilder, 10000000, 1000, 1, 10, true,  PublishConfiguration.PublisherType.ASYNC));
        builder.append(runTests(baseBuilder, 10000000, 1000, 1, 100, true,  PublishConfiguration.PublisherType.ASYNC));
        builder.append(runTests(baseBuilder, 10000000, 1000, 1, 1000, true,  PublishConfiguration.PublisherType.ASYNC));
*/



    }


    private String runTests( EventHubConfiguration baseBuilder, int currentMessageCount, int currentMessageSize, int currentSubscriberCount, int batchSize, boolean acksEnabled, PublishConfiguration.PublisherType publisherType) throws EventHubClientException, InterruptedException {

        EventHubConfiguration publisherConfiguration = baseBuilder.cloneConfig()
                .publishConfiguration(new PublishConfiguration.Builder()
                        .publisherType(publisherType)
                        .build())
                .build();
        EventHubConfiguration subscribeConfig = baseBuilder.cloneConfig()
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                    .acksEnabled(acksEnabled)
                    .batchingEnabled(batchSize != 0)
                    .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                    .subscriberName(subscriberName)
                    .batchSize(batchSize==0 ? 1 : batchSize)
                    .build())
                .build();


        ArrayList<Client> subscribeClients = new ArrayList<>();
        Client publishClient = new Client(publisherConfiguration);
        CountDownLatch publisherAndSubscriberReady = new CountDownLatch(2);
        String messageBody = createRandomString(currentMessageSize / 2);
        for (int j = 0; j < currentSubscriberCount; j++) {
            subscribeClients.add(new Client(subscribeConfig));
        }
        final AtomicLong pubFinishTime = new AtomicLong(0);
        final AtomicLong subFinishTime = new AtomicLong(0);
        final AtomicLong firstSubReceiveTime = new AtomicLong(0);
        final AtomicLong firstPubReceiveTime = new AtomicLong(0);
        final AtomicInteger totalPublishCount = new AtomicInteger(0);

        final AtomicBoolean pubTestPass = new AtomicBoolean(false);
        final AtomicBoolean subTestPass = new AtomicBoolean(false);


        Thread publisherThread = new Thread(() -> {
            try {
                PublishCallback pubCallback = new PublishCallback();
                if (publisherConfiguration.getPublishConfiguration().isAsyncConfig()) {
                    publishClient.registerPublishCallback(pubCallback);
                    buildAndSendMessages(publishClient, messageBody, 1);
                    pubCallback.block(1);
                    pubCallback.resetCounts();
                } else {
                    buildAndSendMessages(publishClient, messageBody, 1);
                }
                System.out.println("pub ready and waiting");
                publisherAndSubscriberReady.countDown();
                publisherAndSubscriberReady.await();

                int numberSent = 0;
                List<Ack> acksList = new ArrayList<>();
                totalPublishCount.set(0);
                while (currentMessageCount > numberSent) {
                    int numberToSend = (int) Math.min(Math.min(currentMessageCount - numberSent, publisherConfiguration.getPublishConfiguration().getMaxAddedMessages()), (maxMessageSizeBytes * .9) / currentMessageSize);
                    numberSent += numberToSend;
                    totalPublishCount.incrementAndGet();
                    acksList.addAll(buildAndSendMessages(publishClient, messageBody, numberToSend));
                    if(!publisherConfiguration.getPublishConfiguration().isAsyncConfig()){
                        System.out.println("sync pub published: " + numberToSend + "\ttotal count sent: " + numberSent);
                    }

                }
                System.out.println("pub done, number of publish requests: " + totalPublishCount.get());
                if (publisherConfiguration.getPublishConfiguration().isAsyncConfig()) {
                    pubCallback.block(currentMessageCount);
                    firstPubReceiveTime.set(pubCallback.firstAckTime.get());
                }
                if (currentMessageCount != (publisherConfiguration.getPublishConfiguration().isAsyncConfig() ? pubCallback.receivedAcks.get() : acksList.size())) {
                    System.out.println(String.format("got %d/%d acks in pub: ", publisherConfiguration.getPublishConfiguration().isAsyncConfig() ? pubCallback.receivedAcks.get() : acksList.size(), currentMessageCount, publisherConfiguration.getPublishConfiguration().toString()));
                }else{
                    pubTestPass.set(true);
                }

            } catch (EventHubClientException | InterruptedException e) {
                e.printStackTrace();
            }
            pubFinishTime.set(System.currentTimeMillis());

        });

        Thread subscriberTimeThread = new Thread(() -> {
            try {
                MasterSubscribeCallback mc = new MasterSubscribeCallback();
                for (Client c : subscribeClients) {
                    mc.addClient(batchSize!=0, acksEnabled, c);
                }
                mc.block(1);
                System.out.println("drained " + mc.receivedMessages.get());
                mc.resetCount();
                System.out.println("sub ready");
                publisherAndSubscriberReady.countDown();
                mc.block(currentMessageCount);
                if(currentMessageCount == mc.receivedMessages.get()){
                    subTestPass.set(true);
                }
                firstSubReceiveTime.set(mc.firstReceiveTime.get());

            } catch (EventHubClientException | InterruptedException e) {
                e.printStackTrace();
            }

            subFinishTime.set(System.currentTimeMillis());
        });
        subscriberTimeThread.start();
        pause(TestUtils.SUBSCRIBER_ACTIVE_WAIT_LENGTH * 2);
        publisherThread.start();

        publisherAndSubscriberReady.await();
        long startTime = System.currentTimeMillis();

        publisherThread.join();
        subscriberTimeThread.join();

        long publishDuration = pubFinishTime.get() - startTime;
        long subscribeDuration = subFinishTime.get() - startTime;
        long firstPubTimeDuration = firstPubReceiveTime.get() == 0 ? 0 - 1 : firstPubReceiveTime.get() - startTime;
        long firstSubTimeDuration = firstSubReceiveTime.get() == 0 ? 0 - 1 : firstSubReceiveTime.get() - startTime;



        publishClient.shutdown();
        for (Client c : subscribeClients) {
            c.unsubscribe();
            c.shutdown();
        }

        return  EventHubUtils.formatJson(
                "pub_config", publisherConfiguration.getPublishConfiguration().toString(),
                "sub_config", subscribeConfig.getSubscribeConfiguration().toString(),
                "success", pubTestPass.get() && subTestPass.get(),
                "num_messages", currentMessageCount,

                "individual_message_size", currentMessageSize,
                "number_publishes", totalPublishCount,
                "number_subscribers", currentSubscriberCount,

                "pub_total_length", publishDuration,
                "pub_first_ack_rtt", firstPubTimeDuration,
                "sub_total_length", subscribeDuration,
                "sub_first_ack_rtt", firstSubTimeDuration,
                "sub_message_bandwidth_kBs", subTestPass.get() ? ((currentMessageSize * currentMessageCount) / 1024.0) /  (subscribeDuration / 1000.0) : 0,
                "sub_message_sec", subTestPass.get() ? currentMessageCount  /  (subscribeDuration / 1000.0) : 0,
                "pub_message_bandwidth_kBs", subTestPass.get() ? ((currentMessageSize * currentMessageCount) / 1024.0) /  (publishDuration / 1000.0) : 0,
                "pub_message_sec", subTestPass.get() ? currentMessageCount /  (publishDuration / 1000.0) : 0

                ).toString(3);
    }

    void pause(Long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            System.out.println("~~~~~ TIMEOUT INTERRUPTED ~~~~~");
        }
    }

}
