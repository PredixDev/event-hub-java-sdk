/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub.client;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLException;

import com.ge.predix.eventhub.EventHubLogger;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.LoggerConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import io.grpc.*;
import io.grpc.internal.ManagedChannelImpl;
import org.junit.FixMethodOrder;
import org.junit.Ignore;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.ge.predix.eventhub.EventHubClientException;
import org.junit.runners.MethodSorters;
@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BackOffDelayTest {
    static EventHubLogger ehLogger = new EventHubLogger(BackOffDelay.class, LoggerConfiguration.defaultLogConfig());
    static class MockBackOffDelay extends BackOffDelay {
        private final int[] delayMillisecondsOverride = {10, 50, 100, 500, 3000, 5000}; // 10ms, 50ms, 100ms, 0.5s, 3s, 5s


        public MockBackOffDelay(ClientInterface client) {
            super(client, ehLogger);

// Overriding timeouts for the test so it is faster
            delayMilliseconds = delayMillisecondsOverride;
            timeForReset = 30000L; // 30s
        }
    }

    static class MockSyncPublishClient extends SyncPublisherClient {
        public MockSyncPublishClient(Channel channel, ManagedChannel originChannel, EventHubConfiguration eventHubConfiguration) throws SSLException {
//channel and client

            super(channel,  originChannel, eventHubConfiguration);
            backOffDelay = new MockBackOffDelay(this);
        }
    }

    static class MockAsyncPublishClient extends AsyncPublishClient {
        public MockAsyncPublishClient(Channel channel, EventHubConfiguration eventHubConfiguration) {
//channel and client
            super(channel, null, eventHubConfiguration);
            backOffDelay = new MockBackOffDelay(this);
        }
    }

    // This is used as a barebones PublishClient for us to capture createStream() being called by BackOffDelay
// It records the number of times createStream() is called with reconnectStreamCount
// It allows attemptReconnect() to be called after certain attempts with the array countsToReconnectOn
    class MockClient extends ClientInterface {

        private AtomicInteger reconnectStreamCount = new AtomicInteger(0);
        private Iterator<Status> reconnectStatusesFromEventHub;  // Simulated status from Event hub after calling reconnectStream (aka if we want the reconnect to failed with Status.UNAVAILABLE over and over)
        private ArrayList<Integer> backOffTimeDelays = new ArrayList<Integer>(); // Stores the time delay given for
        protected MockBackOffDelay backOffDelay;


        public MockClient(ArrayList<Status> reconnectStatusesFromEventHub) throws EventHubClientException.ReconnectFailedException {
            this.reconnectStatusesFromEventHub = reconnectStatusesFromEventHub == null ? new ArrayList<Status>().iterator() : reconnectStatusesFromEventHub.iterator();
            this.backOffDelay = new MockBackOffDelay(this);

// Make first status response from Event Hub
            if (this.reconnectStatusesFromEventHub.hasNext()) {
                this.onErrorStatusFromEventHub(this.reconnectStatusesFromEventHub.next());
            }
        }

        // This mocks any errors that would arrive from Event Hub (we only care about the status)
        public int onErrorStatusFromEventHub(Status status) throws EventHubClientException.ReconnectFailedException {
// Use status to initiate a reconnect and store the delay time in array
            int delay = this.backOffDelay.initiateReconnect(status);
            this.backOffTimeDelays.add(delay);
            return delay;
        }

        // This mocks the Client method that eventually gets called from BackoffDelay
// Check iterator to see if we should simulate a response from Event Hub
        protected synchronized void reconnectStream(String cause) {
            this.reconnectStreamCount.incrementAndGet();

            if (this.reconnectStatusesFromEventHub.hasNext()) {
                Status status = this.reconnectStatusesFromEventHub.next();
//System.out.println("Event Hub onFailure will be called with " + status.getCode());
                try {
                    this.onErrorStatusFromEventHub(status);
                } catch (EventHubClientException.ReconnectFailedException e) {
//The MockClient does not have a callback to call onFailure (we have this outside of tests)
//System.out.println("In reconnectStream " + e.getLocalizedMessage());
                }
            } else {
//          System.out.println("No more statuses from Event Hub ... reconnection simulated to be successful");
            }
        }

        protected void throwErrorToStream(Status.Code code, String description, Throwable cause) {
            Status status = io.grpc.Status.fromCode(code).withDescription(description).withCause(cause);
            try {
                this.onErrorStatusFromEventHub(status);
            } catch (EventHubClientException.ReconnectFailedException e) {
                System.out.println("In reconnectStream " + e.getLocalizedMessage());
            }
        }

        protected boolean isStreamClosed() {
            return true;
        }

        protected boolean isClientActive() {
            return true;
        }

        public int getReconnectStreamCount() {
            return this.reconnectStreamCount.get();
        }

        public ArrayList<Integer> getBackOffTimeDelays() {
            return this.backOffTimeDelays;
        }

    }

    // Find percent difference between real and actual given delay index and BackOffDelay
    Double calculateJitter(BackOffDelay backOffDealy, int attenptZeroIndex, int actual) {
        int delayMillisecondsIndex = attenptZeroIndex % backOffDealy.delayMilliseconds.length; // Essentially BackOffDelay.getDelay(i)
        int jitterAmount = Math.abs(backOffDealy.delayMilliseconds[delayMillisecondsIndex] - actual);
        return jitterAmount * 1.0 / backOffDealy.delayMilliseconds[delayMillisecondsIndex];
    }

    // Get a timeout we should wait so that all the retries would have finished
    static long calculatePause(BackOffDelay backOffDelay, int startIndex, int numberOfDelays) {
// Wait for how long the above timeouts should take
        Long waitFor = 0L;
        for (int i = 0; i < numberOfDelays; i++) {
            waitFor += backOffDelay.getDelay(i + startIndex);
        }
// Augment waitFor by the max jitter since the total timeout could be more than the simple sum of the array
        Double jitterTimes100 = backOffDelay.maxJitterFactor * 100;  // maxJitter is a decimal percentage ...
        return waitFor + (waitFor * jitterTimes100.longValue()) / 100;   // total + percentage defined by jitter
    }

    void pause(Long timeout) {
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

    @Test
// If attempt number is over length of array, getDelay() should wrap around (aka mod length)
    public void getDelayWrapAround() throws EventHubClientException {
        BackOffDelay backOffDelay = new BackOffDelay(new MockClient(new ArrayList<>()), ehLogger);
        assertEquals(BackOffDelay.delayMilliseconds[0], backOffDelay.getDelay(BackOffDelay.delayMilliseconds.length));
    }

    @Test
// Reconnect multiple times
    public void multipleReconnect() throws SSLException, EventHubClientException {
        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        statusResponseFromEventHub.add(Status.UNAVAILABLE);   // 1st response from Event Hub
        statusResponseFromEventHub.add(Status.UNAVAILABLE);   // ...
        statusResponseFromEventHub.add(Status.UNAVAILABLE);
        statusResponseFromEventHub.add(Status.UNAVAILABLE);
        statusResponseFromEventHub.add(Status.UNAVAILABLE);
        statusResponseFromEventHub.add(Status.UNAVAILABLE);   // 6th reconnect (should be last delay in MockBackOffDelay)
        statusResponseFromEventHub.add(Status.UNAVAILABLE);   // 7th reconnect should have the same delay as the 1st
        statusResponseFromEventHub.add(Status.UNAVAILABLE);   // 8th reconnect should have the same delay as the 1st
        statusResponseFromEventHub.add(Status.UNAVAILABLE);   // ...

        MockClient client = new MockClient(statusResponseFromEventHub);

        pause(calculatePause(client.backOffDelay, 0, statusResponseFromEventHub.size()));

// Get delays which were used
        ArrayList<Integer> delays = client.getBackOffTimeDelays();

// Make sure time delays jitters are within expected range
        for (int i = 0; i < delays.size(); i++) {
// Ensure that we read either the correct index since the delays wrap around
            Double jitterFactor = calculateJitter(client.backOffDelay, i, delays.get(i));
            assertThat(BackOffDelay.maxJitterFactor, greaterThanOrEqualTo(jitterFactor));
        }
    }

    @Test
// If reconnect request made during another request, it should be ignored
    public void ignoreReconnect() throws SSLException, EventHubClientException {

        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        statusResponseFromEventHub.add(Status.UNAVAILABLE);
        MockClient client = new MockClient(statusResponseFromEventHub); // Trigger 1st retry

        pause(200L);

// Force call reconnect twice back to back
        int secondRetryDelay = client.onErrorStatusFromEventHub(Status.UNAVAILABLE);  // Trigger 2nd
        int thirdRetryDelay = client.onErrorStatusFromEventHub(Status.UNAVAILABLE);  // Trigger 3rd but should fail since it was called during past reconnect

        pause(2000L);
        assertThat(secondRetryDelay, greaterThanOrEqualTo(1)); // Ensure that there was a reconnect
        assertEquals(0, thirdRetryDelay); // Ensure no reconnect attempt on third try
        assertEquals(1 + 1, client.getReconnectStreamCount());
    }

    @Test
// If it has been longer than the timeout since last reconnect, reset the attempt index
    public void resetAttemptAfterTimeout() throws SSLException, EventHubClientException {
        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        statusResponseFromEventHub.add(Status.UNAVAILABLE); // Trigger retry
        statusResponseFromEventHub.add(Status.UNAVAILABLE); // Trigger retry
        MockClient client = new MockClient(statusResponseFromEventHub);

        pause(100L);
        assertEquals(2, client.backOffDelay.attempt.get()); // Check BackOffDelay internal index
        assertEquals(2, client.getReconnectStreamCount());  // Check number of times client was to reconnect
        assertEquals(2, client.getBackOffTimeDelays().size());  // Check number of times reconnect was initiated

        pause(client.backOffDelay.timeForReset);
        assertEquals(2, client.backOffDelay.attempt.get());  // Make sure attempts did not change

        int afterResetTimeoutDelay = client.onErrorStatusFromEventHub(Status.UNAVAILABLE);
        assertThat(afterResetTimeoutDelay, greaterThanOrEqualTo(1)); // Ensure delay is happening (if 0, then delay reconnect not called)
        pause(100L);
        assertEquals(1, client.backOffDelay.attempt.get());  // #0 for call, but checking after call so index should be 1
        assertEquals(3, client.getReconnectStreamCount());  // From client side, numbers should increment
        assertEquals(3, client.getBackOffTimeDelays().size());  // From client side, numbers should increment
    }

    @Test
// Retry initially null, takes on int value when assign if it was null
// Ignores new assignments if not null
// Can be reset if last retryLimit was set more than time delay
    public void retryLimit() throws EventHubClientException.ReconnectFailedException {
        MockClient client = new MockClient(null);
        assertEquals(null, client.backOffDelay.retryLimit);

// Set limit
        int limit = 5;
        client.backOffDelay.setRetryLimit(limit);
        assertEquals(limit, client.backOffDelay.retryLimit.get());

// Tries to set limit again (should not work)
        int newLimit = 7;
        client.backOffDelay.setRetryLimit(newLimit);
        assertEquals(limit, client.backOffDelay.retryLimit.get());

// Wait to set limit again
        pause(client.backOffDelay.timeForReset + 100L);

// Tries to set NEW limit again (using a even higher limit to prove that the timeout function works)
        int newerLimit = 20;
        client.backOffDelay.setRetryLimit(newerLimit);
        assertEquals(newerLimit, client.backOffDelay.retryLimit.get());

// Remove delay
        client.backOffDelay.removeRetryLimit();
        assertEquals(null, client.backOffDelay.retryLimit);

    }

    //If retryLimit is set, try until no retries left
    @Test(expected = EventHubClientException.ReconnectFailedException.class)
    public void retryLimitWithClient() throws EventHubClientException.ReconnectFailedException {
        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        statusResponseFromEventHub.add(Status.UNKNOWN);   // 1st response that sets retryLimit and delay index bumped up

// Add up to max retryLimit (the 1st retry is included in limit)
        for (int i = 0; i < MockBackOffDelay.limitedRetryLimit - 1; i++) {
            statusResponseFromEventHub.add(Status.UNKNOWN);
        }

// Add a couple retries but should not go through since it is over the limit
        statusResponseFromEventHub.add(Status.UNKNOWN); // This should not trigger reconnectStream
        statusResponseFromEventHub.add(Status.UNKNOWN); // This should never even be triggered

        MockClient client = new MockClient(statusResponseFromEventHub);

        pause(calculatePause(client.backOffDelay, client.backOffDelay.midLengthDelayIndex, statusResponseFromEventHub.size()));

        client.onErrorStatusFromEventHub(Status.UNKNOWN);
        pause(100L);
        assertEquals(client.backOffDelay.limitedRetryLimit, client.getBackOffTimeDelays().size());
        assertEquals(client.backOffDelay.limitedRetryLimit, client.getReconnectStreamCount());  // Make sure we stopped calling reconnect

    }

    @Test
// If there is an error that does not set retryLimit, it should reset limit (since it is a new error)
    public void retryLimitReset() throws EventHubClientException.ReconnectFailedException {
        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        statusResponseFromEventHub.add(Status.UNKNOWN);   // 1st response that sets retryLimit and delay index bumped up
        statusResponseFromEventHub.add(Status.UNKNOWN);

        MockClient client = new MockClient(statusResponseFromEventHub);

        pause(calculatePause(client.backOffDelay, client.backOffDelay.midLengthDelayIndex, statusResponseFromEventHub.size()));
        assertEquals(client.backOffDelay.limitedRetryLimit - 2, client.backOffDelay.retryLimit.get());  // Make sure retryLimit is being used

//  This should reset retryLimit
        client.backOffDelay.removeRetryLimit();

        pause((long) client.backOffDelay.delayMilliseconds[2]);
        assertEquals(null, client.backOffDelay.retryLimit); // No more retryLimit

    }

    @Test
// Specify index
    public void specifyIndex() throws EventHubClientException.ReconnectFailedException {
        MockClient client = new MockClient(null);
        int index = 3;
        int delayTime = client.backOffDelay.attemptReconnect(index);
        Double jitter = calculateJitter(client.backOffDelay, index, delayTime);
        assertThat(client.backOffDelay.maxJitterFactor, greaterThanOrEqualTo(jitter));
    }

    /**
     * This next section runs through scenarios with different Status.Codes
     */

    @Test
// Should do nothing
    public void status_CANCELLED() throws EventHubClientException.ReconnectFailedException {
        MockClient client = new MockClient(null);
        client.onErrorStatusFromEventHub(Status.CANCELLED);

        pause(calculatePause(client.backOffDelay, 0, 1));
        assertEquals(1, client.getBackOffTimeDelays().size());      // Ensure there was 1 reconnect initiated
        assertEquals(0, client.getReconnectStreamCount());   // Ensure client's reconnect did not get called
    }

    @Test
// Should stop after x amount of tries
    public void status_UNKNOWN() throws EventHubClientException.ReconnectFailedException {
        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        statusResponseFromEventHub.add(Status.UNKNOWN);   // 1st response that sets retryLimit and delay index bumped up
        statusResponseFromEventHub.add(Status.UNKNOWN);
        statusResponseFromEventHub.add(Status.UNKNOWN);
        statusResponseFromEventHub.add(Status.UNKNOWN);   // 4th call will trigger an error to Client but that will not reach reconnect

        MockClient client = new MockClient(statusResponseFromEventHub);

        pause((long) client.backOffDelay.delayMilliseconds[client.backOffDelay.limitedRetryLimit] * 4);

        try {
            client.onErrorStatusFromEventHub(Status.UNKNOWN);
        } catch (EventHubClientException.ReconnectFailedException e) {
            pause(100L);
// getBackOffTimeDelays.size() will be 3 since on the fourth try an error is thrown
            assertEquals(statusResponseFromEventHub.size(), client.getBackOffTimeDelays().size() + 1);
            assertEquals(client.backOffDelay.limitedRetryLimit, client.getReconnectStreamCount());
        }
    }

    public void checkStatusForMaxRetries(Status status) throws EventHubClientException.ReconnectFailedException {
        int repeat = BackOffDelay.maxRetryLimit + 3; // This will be more than maxRetryLimit to make sure the limit is set
        ArrayList<Status> statusResponseFromEventHub = new ArrayList<Status>();
        for (int i = 0; i < repeat; i++) {
            statusResponseFromEventHub.add(status);
        }

        MockClient client = new MockClient(statusResponseFromEventHub);

        pause(calculatePause(client.backOffDelay, 0, repeat));
        assertEquals(BackOffDelay.maxRetryLimit, client.getBackOffTimeDelays().size());  // Check error call count (max + original call)
        assertEquals(BackOffDelay.maxRetryLimit, client.getReconnectStreamCount()); // Check reconnect call count (the call at the limit will not trigger retry)
        for (int i = 0; i < BackOffDelay.maxRetryLimit; i++) {  // Check timeout accuracy (skips the last one since delay = 0 for no reconnect called)
            Double jitterFactor = calculateJitter(client.backOffDelay, i, client.getBackOffTimeDelays().get(i));
            assertThat(BackOffDelay.maxJitterFactor, greaterThanOrEqualTo(jitterFactor));
        }
    }

    @Test
// Should keep trying indefinitely
    public void status_UNAVAILABLE() throws EventHubClientException.ReconnectFailedException {
        checkStatusForMaxRetries(Status.UNAVAILABLE);
    }

    @Test
// Should keep trying indefinitely
    public void status_UNAUTHENTICATED() throws EventHubClientException.ReconnectFailedException {
        checkStatusForMaxRetries(Status.UNAUTHENTICATED);
    }

    @Test
// Should keep trying indefinitely
    public void status_INTERNAL() throws EventHubClientException.ReconnectFailedException {
        checkStatusForMaxRetries(Status.INTERNAL);
    }

    @Test
// If two initiateReconnects are called and the error codes produce the same result, one will win
    public void threadingSameStatus() throws InterruptedException, EventHubClientException.ReconnectFailedException {
        final MockClient client = new MockClient(null);

        final CountDownLatch startSync = new CountDownLatch(1);
        Thread error1 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    client.onErrorStatusFromEventHub(Status.UNAVAILABLE);
                } catch (EventHubClientException.ReconnectFailedException e) {
                    System.out.println("error1 thread: " + e.getLocalizedMessage());
                }
            }
        };
        Thread error2 = new Thread(error1);

        error1.start();
        error2.start();
        startSync.countDown();
        error1.join();
        error2.join();

        pause(calculatePause(client.backOffDelay, 0, 2));
        assertEquals(2, client.getBackOffTimeDelays().size());  // Check that both errors were made
        assertThat(client.getReconnectStreamCount(), greaterThanOrEqualTo(1)); // Check that at least one made it through
    }

    @Test
// Make sure jitter is correct
    public void testJitter() throws EventHubClientException.ReconnectFailedException {
        int[] timeout = {300, 100000, 10};
        double[] factor = {0.5, 0.01, 0.1};
        MockClient client = new MockClient(null);

        for (int i = 0; i < timeout.length; i++) {
            double maxJitter = timeout[i] * factor[i];
            for (int x = 0; x < 100; x++) {
                int timeoutWithJitter = client.backOffDelay.addJitter(timeout[i], factor[i]);
                int jitterAmount = Math.abs(timeoutWithJitter - timeout[i]);
                assertThat(maxJitter, greaterThanOrEqualTo((double) jitterAmount));
            }
        }
    }

    @Test
// Make sure setRetryLimit can only be initially set once
    public void testSetRetryLimit() throws InterruptedException, EventHubClientException.ReconnectFailedException {
        final MockClient client = new MockClient(null);
        final int retryLimit = 5;

        final CountDownLatch startSync = new CountDownLatch(1);

        Thread thread1 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                client.backOffDelay.setRetryLimit(retryLimit);
            }
        };

        Thread thread2 = new Thread() {
            public void run() {
                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                client.backOffDelay.setRetryLimit(retryLimit);
            }
        };

        thread1.start();
        thread2.start();
        startSync.countDown();
        thread1.join();
        thread2.join();

        assertEquals(retryLimit, client.backOffDelay.retryLimit.get());
        pause(30000L);  // Wait for reset timeout

        client.backOffDelay.setRetryLimit(retryLimit + 1);
        assertEquals(retryLimit + 1, client.backOffDelay.retryLimit.get());

    }

    @Test
// Make sure that a smaller retryLimit can override a larger retryLimit
    public void smallerRetryLimit() throws EventHubClientException.ReconnectFailedException {
        MockClient client = new MockClient(null);
        client.backOffDelay.setRetryLimit(10);
        assertEquals(10, client.backOffDelay.retryLimit.get());

// Larger limit
        client.backOffDelay.setRetryLimit(11);
        assertEquals(10, client.backOffDelay.retryLimit.get());

// Smaller limit
        client.backOffDelay.setRetryLimit(2);
        assertEquals(2, client.backOffDelay.retryLimit.get());
    }

    @Test
    public void publishSyncReconnectException() throws EventHubClientException, SSLException {

        EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .build();

        MockClient mockClient = new MockClient(null);

//create bad channel
        Client syncClient = new Client(eventHubConfiguration);

        ChannelTuple channelTuple = buildNonWorkingChannel(eventHubConfiguration, syncClient);

        MockSyncPublishClient mockSyncPublishClient = buildNoRetryPublishClient(eventHubConfiguration, mockClient, channelTuple.channel, channelTuple.managedChannel);

        syncClient.publishClient = mockSyncPublishClient;

//Send first invalid message, then wait for reconnect to fail
        syncClient.addMessage("id", "message", null);
        try{
             syncClient.flush();
        }catch (EventHubClientException.SyncPublisherFlushException ignore){

        }
        pause(5000L);

//This call should trigger the reconnect failure exception
        syncClient.addMessage("id", "message", null);
        EventHubClientException.SyncPublisherFlushException e = null;
        try {
            syncClient.flush();
        }catch (EventHubClientException.SyncPublisherFlushException e1){
            e = e1;
        }
        assertNotNull(e);
        boolean foundReconnect = false;
        for(Throwable t : e.getThrowables()){
            if(t instanceof  EventHubClientException.ReconnectFailedException){
                foundReconnect= true;
        }
    }
        assertTrue(foundReconnect);
        channelTuple.managedChannel.shutdownNow();
    }

    private MockSyncPublishClient buildNoRetryPublishClient(EventHubConfiguration eventHubConfiguration, MockClient mockClient, Channel channel, ManagedChannel originChannel) {
        MockSyncPublishClient mockSyncPublishClient = null;
        try {
            mockSyncPublishClient = new MockSyncPublishClient(channel, originChannel, eventHubConfiguration);
        } catch (SSLException e) {
            e.printStackTrace();
        }
        MockBackOffDelay backOffDelay = new MockBackOffDelay(mockClient);
        backOffDelay.setRetryLimit(0);
        mockSyncPublishClient.backOffDelay = backOffDelay;
        return mockSyncPublishClient;

        
    }


    private ChannelTuple buildNonWorkingChannel(EventHubConfiguration eventHubConfiguration, Client syncClient) throws SSLException {
        ManagedChannel originChannel = ManagedChannelBuilder.forAddress("non-working-backoff-delay-host", 8080).build();


        HeaderClientInterceptor interceptor = new HeaderClientInterceptor(syncClient, eventHubConfiguration);

        return new ChannelTuple(originChannel, ClientInterceptors.intercept(originChannel, interceptor));
    }

    class ChannelTuple
    {
        ManagedChannel managedChannel;

        Channel channel;
        ChannelTuple(ManagedChannel managedChannel, Channel channel){
            this.managedChannel = managedChannel;
            this.channel = channel;
        }
    }

}
