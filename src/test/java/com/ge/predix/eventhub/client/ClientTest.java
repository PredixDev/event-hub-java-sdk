/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub.client;

import static com.ge.predix.eventhub.client.utils.TestUtils.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLException;

import com.ge.predix.eventhub.EventHubUtils;
import com.ge.predix.eventhub.client.utils.TestUtils;
import com.ge.predix.eventhub.client.utils.TestUtils.*;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.ge.predix.eventhub.Ack;
import com.ge.predix.eventhub.AckStatus;
import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import org.junit.runners.MethodSorters;

@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClientTest {

    final String subscriberName = "test-subscriber";
    final String subscriberID = "tester";

    Client eventHubClientSync;
    Client eventHubClientAsync;
    EventHubConfiguration eventHubConfigurationSync;
    EventHubConfiguration eventHubConfigurationAsync;

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

    @BeforeClass
    public static void beforeClass() {
// Make an instance of BackOffDelay to override static delay array so it is shorter for tests
        BackOffDelayTest.MockBackOffDelay mockClient = new BackOffDelayTest.MockBackOffDelay(null);
    }

    @Before
    public void createClient() throws EventHubClientException {
        eventHubConfigurationSync = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName + "-sync").subscriberInstance(subscriberID).build())
                .build();

        eventHubConfigurationAsync = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName(subscriberName + "-async").subscriberInstance(subscriberID).build())
                .build();
        pause(1000L);
        eventHubClientAsync = new Client(eventHubConfigurationAsync);
        eventHubClientSync = new Client(eventHubConfigurationSync);
    }

    @Test
// Makes sure bad token gets 0 acks and forceRenewToken() will get new token
    public void forceTokenRenew() throws SSLException, EventHubClientException {
//for client to get a token first and create publishClient object
        eventHubClientSync.addMessage("1", "message!", null).flush();

//place bad token and make request which should fail
        eventHubClientSync.setAuthToken(""); //use bad token

        List<Ack> acks = new ArrayList<Ack>();
        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
        try {
            acks = eventHubClientSync.addMessage("1", "I should fail!", null).flush();
        } catch (EventHubClientException.SyncPublisherFlushException e) {
            syncPublisherFlushException = e;
        } finally {
            assertNotNull(syncPublisherFlushException);
            assertEquals(1, syncPublisherFlushException.getThrowables().size());
        }
        assertTrue(acks.isEmpty());

//reset expiration time to force token renew and make request which should pass
        try {
            eventHubClientSync.forceRenewToken();
        } catch (EventHubClientException e) {
            System.out.println(e.getMessage());
        }
        acks = eventHubClientSync.addMessage("1", "I should pass!", null).flush();
        assertEquals(AckStatus.ACCEPTED, acks.get(0).getStatusCode());
    }

    @Test
// if automaticTokenRenew=False then do not get new token when creating new stream
    public void disableTokenRefresh() throws EventHubClientException {
        try {
            EventHubConfiguration configWithNoAuthDetails = new EventHubConfiguration.Builder()
                    .host(eventHubConfigurationSync.getHost())
                    .port(eventHubConfigurationSync.getPort())
                    .zoneID(eventHubConfigurationSync.getZoneID())
                    .automaticTokenRenew(false)
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .build();

            Client eventHubClientNoRenew = new Client(configWithNoAuthDetails);
            try{
                eventHubClientNoRenew.addMessage("1", "message!", null).flush(); // this would have renewed the token on first call
            }catch (EventHubClientException ignore){

            }

            assertEquals("", eventHubClientNoRenew.getAuthToken());

            eventHubClientNoRenew.shutdown();

        } catch (EventHubClientException.InvalidConfigurationException e) {
            System.out.println(e.getMessage());
            fail();
        }
    }

    @Test
// if there are configurations set for AuthURL, clientID or clientSecret, throw exception with forceRenewToken()
    public void forceTokenRenewDisabledRefreshWithoutAuthDetails() {
// renew only if there are auth details
        boolean caught = false;
        try {
            EventHubConfiguration configWithNoAuthDetails = new EventHubConfiguration.Builder()
                    .host(eventHubConfigurationSync.getHost())
                    .port(eventHubConfigurationSync.getPort())
                    .zoneID(eventHubConfigurationSync.getZoneID())
                    .automaticTokenRenew(false)
                    .build();

            Client eventHubClientNoRenew = new Client(configWithNoAuthDetails);
            eventHubClientNoRenew.forceRenewToken();

            eventHubClientNoRenew.shutdown();

        } catch (EventHubClientException e) {
            if (e.getMessage().contains("can't renew token")) {
                caught = true;
            } else {
                System.out.println(e.getMessage());
                fail();
            }
        }
        assertTrue(caught);
    }

    @Test
// Even is automaticTokenRenew=false, forceRenewToken() should get new token for client as long as AuthURL, clientId and clientSecret are there
    public void forceTokenRenewDisabledRefreshWithAuthDetails() {
// renew only if there are auth details
        boolean caught = false;
        try {
            EventHubConfiguration configWithAuthDetails = new EventHubConfiguration.Builder()
                    .host(eventHubConfigurationSync.getHost())
                    .port(eventHubConfigurationSync.getPort())
                    .zoneID(eventHubConfigurationSync.getZoneID())
                    .authURL(eventHubConfigurationSync.getAuthURL())
                    .clientID(eventHubConfigurationSync.getClientID())
                    .clientSecret(eventHubConfigurationSync.getClientSecret())
                    .automaticTokenRenew(false)
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .build();

            Client eventHubClientYesRenew = new Client(configWithAuthDetails);

// should not be getting a token, so this should fail
            EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
            try {
                List<Ack> acks = eventHubClientYesRenew.addMessage("1", "I should fail!", null).flush();
            } catch (EventHubClientException.SyncPublisherFlushException e) {
                syncPublisherFlushException = e;
            } finally {
                assertNotNull(syncPublisherFlushException);
                assertEquals(syncPublisherFlushException.getThrowables().size(), 2);
            }

            eventHubClientYesRenew.forceRenewToken(); // will fail regardless if the details are there or not

            eventHubClientYesRenew.shutdown();

        } catch (EventHubClientException e) {
            if (e.getMessage().contains("can't renew token")) {
                caught = true;
            } else {
                System.out.println(e.getMessage());
                fail();
            }
        }

        assertTrue(caught);
    }

    @Test
// Behavior should be:
//  - Attempt connect but does not get a token (bad response or bad credentials or bad scopes)
//  - Connection immediately cancelled and an UNAUTHENTICATED error is thrown
//  - Reconnect indefinitely (perhaps OAuth issuer is down or the user fixes scopes)
    public void testBadAuth() {
// bad url
        try {
            EventHubConfiguration configBadAuthURL = new EventHubConfiguration.Builder()
                    .host(eventHubConfigurationSync.getHost())
                    .port(eventHubConfigurationSync.getPort())
                    .zoneID(eventHubConfigurationSync.getZoneID())
                    .authURL(eventHubConfigurationSync.getAuthURL() + "bad")
                    .clientID(eventHubConfigurationSync.getClientID())
                    .clientSecret(eventHubConfigurationSync.getClientSecret())
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .build();

            Client eventHubClient = new Client(configBadAuthURL);
            try{
                eventHubClient.addMessage("1", "I should fail!", null).flush();
            }catch (EventHubClientException ignore){

            }
            pause(1000L);
            assertThat(eventHubClient.publishClient.backOffDelay.attempt.get(), greaterThanOrEqualTo(1)); // make sure that we are retrying

            eventHubClient.shutdown();

        } catch (Exception e) {
            fail();
        }

// bad creds
        try {
            EventHubConfiguration configBadSecret = new EventHubConfiguration.Builder()
                    .host(eventHubConfigurationSync.getHost())
                    .port(eventHubConfigurationSync.getPort())
                    .zoneID(eventHubConfigurationSync.getZoneID())
                    .authURL(eventHubConfigurationSync.getAuthURL())
                    .clientID(eventHubConfigurationSync.getClientID())
                    .clientSecret(eventHubConfigurationSync.getClientSecret() + "bad")
                    .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                    .build();

            Client eventHubClient = new Client(configBadSecret);
            try{
                eventHubClient.addMessage("1", "I should fail!", null).flush();
            }catch (EventHubClientException ignore){

            }
            pause(2000L);
            assertThat(eventHubClient.publishClient.backOffDelay.attempt.get(), greaterThanOrEqualTo(1)); // make sure that we are retrying

            eventHubClient.shutdown();

        } catch (Exception e) {
            fail();
        }
    }

    @Test
// Ensure that forceRenewToken() works while another thread is sending messages
    public void renewTokenThreadingPub() throws Exception {

        final CountDownLatch startSync = new CountDownLatch(1);
        final AtomicInteger t1Count = new AtomicInteger(0);

        String oldToken = eventHubClientSync.getAuthToken();
        List<Ack> acks = eventHubClientSync.addMessage("message-", "message from addMessage()", null).flush();
        assertEquals(1, acks.size());
        final String originalToken = eventHubClientSync.getAuthToken();

        Thread sendThread = new Thread() {
            public void run() {

                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    for (int i = 0; i < 2; i++) {
                        List<Ack> acks1 = eventHubClientSync.addMessage("message-" + i, "message from addMessage()", null).flush();
                        t1Count.set(t1Count.intValue() + acks1.size());
                    }
                } catch (EventHubClientException e) {
                    fail();
                }

                pause(1000L);

                try {
                    for (int i = 0; i < 2; i++) {
                        List<Ack> acks2 = eventHubClientSync.addMessage("message-" + i, "message from addMessage()", null).flush();
                        t1Count.set(t1Count.intValue() + acks2.size());
                    }
                } catch (EventHubClientException e) {
                    fail();
                }

// Token should have been changed by other thread
                assertNotEquals(originalToken, eventHubClientSync.getAuthToken());
            }
        };

        Thread renewToken = new Thread() {
            public void run() {

                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                pause(500L);
                try {
                    eventHubClientSync.forceRenewToken();
                } catch (EventHubClientException e) {
                    System.out.println(e.getMessage());
                }
            }
        };

        sendThread.start();
        renewToken.start();
        startSync.countDown();
        sendThread.join();
        renewToken.join();
        String newToken = eventHubClientSync.getAuthToken();

// make token was renewed
        assertNotEquals(oldToken, newToken);
    }

    @Test
// Ensures that forceRenewToken() works when subscribing for messages. Subscribe is already threaded
    public void renewTokenSub() throws Exception {
        final String message = "subscribe threading";
        TestSubscribeCallback callback = new TestSubscribeCallback( message);
        eventHubClientSync.subscribe(callback);

// clear queue
        pause(2000L);

        callback.resetCounts();

        String oldToken = eventHubClientSync.getAuthToken();
        List<Ack> acks1 = eventHubClientSync.addMessage("1", message, null).flush();
        assertEquals(1, acks1.size());
        try {
            eventHubClientSync.forceRenewToken();
        } catch (EventHubClientException e) {
            System.out.println(e.getMessage());
        }

// wait a little bit for reconnect after token renew
        pause(1000L);

        String newToken = eventHubClientSync.getAuthToken();
        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
        try {
            List<Ack> acks2 = eventHubClientSync.addMessage("2", message, null).flush();
        } catch (EventHubClientException.SyncPublisherFlushException e) {
            syncPublisherFlushException = e;
        } finally {
            assertNull(syncPublisherFlushException);
        }

//assertEquals(1,acks2.size());
        callback.block(2);
// make sure token was renewed
        assertNotEquals(oldToken, newToken);
// make sure both messages are received
        assertEquals(2, callback.getMessageCount());
    }

    @Test
    public void healthCheck() throws EventHubClientException {
        String message = "checking keep alive message!";
        TestSubscribeCallback callback = new TestSubscribeCallback( message);
        eventHubClientSync.subscribe(callback);

// clear queue
        pause(5000L);
        callback.resetCounts();

        List<Ack> acks = eventHubClientSync.addMessage("keep alive", message, null).flush();
        assertTrue(acks.get(0).getStatusCode() == AckStatus.ACCEPTED);

// wait for HAProxy to close connection
        pause(65000L);

// make sure publish still works
        acks = eventHubClientSync.addMessage("keep alive", message, null).flush();
        assertTrue(acks.get(0).getStatusCode() == AckStatus.ACCEPTED);

        callback.block(2);

// make sure subscribe still works
        assertEquals(2, callback.getMessageCount());
    }

    @Test
// ensure the scope generator methods is correct
    public void generateScopes() throws EventHubClientException {
        String topic1 = "subtopic_1";
        String topic2 = "subtopic_2";
        ArrayList<String> topics = new ArrayList();
        topics.add(topic1);
        topics.add(topic2);

        String subscribeScope =".grpc.subscribe" ;
        String publishScope = ".grpc.publish";
        String userScope = ".user";


        HashMap<EventHubConfiguration, String[]> expectedScopesFromConfigurations = new HashMap<>();

        EventHubConfiguration publishDefaultOnlyConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .build();

        expectedScopesFromConfigurations.put(publishDefaultOnlyConfiguration, new String[]{userScope, publishScope});

        EventHubConfiguration subscribeDefaultOnlyConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .build();
        expectedScopesFromConfigurations.put(subscribeDefaultOnlyConfiguration, new String[]{userScope,subscribeScope});

        EventHubConfiguration publishAndSubscribeDefaultOnlyConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .build();

        expectedScopesFromConfigurations.put(publishAndSubscribeDefaultOnlyConfiguration, new String[]{userScope, publishScope, subscribeScope});


        EventHubConfiguration publishSubtopicOnlyConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).topic(topic1).build())
                .build();

        expectedScopesFromConfigurations.put(publishSubtopicOnlyConfiguration, new String[]{userScope, "."+topic1+publishScope});

        EventHubConfiguration subscribeSubtopicOnlyConfiguration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder().topic(topic1).build())
                .build();

        expectedScopesFromConfigurations.put(subscribeSubtopicOnlyConfiguration, new String[]{userScope, "."+topic1+subscribeScope});

        EventHubConfiguration publishAndSubscribeSubtopics = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).topic(topic1).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().topics(topics).build())
                .build();
        expectedScopesFromConfigurations.put(publishAndSubscribeSubtopics, new String[]{
                userScope,
                "." + topic1 + subscribeScope,
                "." + topic2 + subscribeScope,
                "." + topic1 + publishScope});


        for(EventHubConfiguration config: expectedScopesFromConfigurations.keySet()){
            HeaderClientInterceptor interceptor = new HeaderClientInterceptor(null, config);
            String generatedScopes =  interceptor.generateScopes();
            assertEquals(expectedScopesFromConfigurations.get(config).length, generatedScopes.split(",").length);
            for(String expectedScope: expectedScopesFromConfigurations.get(config)){
                String fullScope =  interceptor.scopePrefix + config.getZoneID() + expectedScope;
                assertTrue(generatedScopes.contains(fullScope));
            }
        }
    }

    @Test
// make sure bad scopes fail correctly
    public void badScopesInConfiguration() throws EventHubClientException {
        EventHubConfiguration configuration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .authScopes("some_bad_scope")
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .build();

        Client client = new Client(configuration);

        boolean caught = false;
        try {
            client.forceRenewToken();
        } catch (EventHubClientException.AuthenticationException e) {
            caught = true;
        }

        assertTrue(caught);

// try publishing (should not work)
        PublishCallback pubCallback = new PublishCallback();
        client.registerPublishCallback(pubCallback);
        client.addMessage("id", "message", null).flush();
        pause(1500L);
        assertThat(pubCallback.getErrorCount(), greaterThanOrEqualTo(2));

// try subscribing (should not work)
        TestSubscribeCallback subCallback = new TestSubscribeCallback( "expected message");
        client.subscribe(subCallback);
        pause(1500L);
        assertThat(subCallback.getErrorCount(), greaterThanOrEqualTo(2));

        client.shutdown();
    }

    /* The client, event-hub-sdk-bad, does not have event hub scopes so is unable
    to be authorized.
    */
    @Test(expected = EventHubClientException.AuthenticationException.class)
    public void testNoValidScopes() throws EventHubClientException, IOException {

        String badClientSecret = EventHubUtils.getValueFromEnv("BAD_CLIENT_SECRET");
        String badClientID = EventHubUtils.getValueFromEnv("BAD_CLIENT_ID");

        EventHubConfiguration configuration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .clientID(badClientID).clientSecret(badClientSecret)
                .build();

        Client client = new Client(configuration);
        client.forceRenewToken();

        client.shutdown();
    }

    @Test
// make sure scopes passes correctly
    public void goodScopesInConfiguration() throws EventHubClientException {
        String scopes = eventHubClientSync.interceptor.scopePrefix + eventHubClientSync.configuration.getZoneID() +".grpc.publish" + ","
                + eventHubClientSync.interceptor.scopePrefix + eventHubClientSync.configuration.getZoneID() +".user";
        EventHubConfiguration configuration = new EventHubConfiguration.Builder().fromEnvironmentVariables()
                .authScopes(scopes)
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .build();

        Client client = new Client(configuration);

// try publishing (should work)
        List<Ack> acks = client.addMessage("id", "some message", null).flush();
        assertEquals(1, acks.size());

// try subscribing (should not work)
        TestSubscribeCallback callback = new TestSubscribeCallback("expected message");
        client.subscribe(callback);
        pause(2000L);
        assertThat(callback.getErrorCount(), greaterThanOrEqualTo(1));

        client.shutdown();
    }

    @Test
// If client is actually connected (perhaps forceTokenRenenw of setAuthToken forced an immediate successful reconnect)
// then do not reconnect after timeout (this is so that the successful connection is not disconnected)
/**
 * @TODO Breaks here publish count
 */
    public void cancelReconnectIfAlreadyConnected() throws EventHubClientException {
// Using a working client,
        PublishCallback pubCallback = new PublishCallback();

        eventHubClientAsync.registerPublishCallback(pubCallback);
        eventHubClientAsync.flush();
        eventHubClientAsync.publishClient.backOffDelay.attempt.set(4);  // Should be 3000ms or 3s in MockBackOffDelay
// Manually force reconnect but make the timeout very long
        Status status = io.grpc.Status.fromCode(Status.Code.UNAVAILABLE).withDescription(null).withCause(new Throwable("java.io.IOException: Connection reset by peer"));
        pubCallback.resetCounts();
        eventHubClientAsync.publishClient.streamObserver.onError(new StatusException(status));   // throw an error that will cause the SDK to reconnect
        pause(200L);

// Make sure that reconnect is happening
        assertEquals(2, pubCallback.getErrorCount()); //one for the error that was thrown and one for the close of stream
        pubCallback.resetCounts();
        assertTrue(eventHubClientAsync.publishClient.isStreamClosed());


// Reconnect while the delay is happening
        eventHubClientAsync.forceRenewToken();

// Make sure reconnection did not happen and the connection made by forceRenewToken is sustained
        pause(3000L);
        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertEquals(0, pubCallback.getErrorCount());

    }

    @Test
// If reconnect is called, do not reconnect clients that have not been initialized or shutdown
    public void reconnectOnlyActiveClients() throws EventHubClientException {

        PublishCallback pubCallback = new PublishCallback();
        TestSubscribeCallback subCallback = new TestSubscribeCallback( "no_expected_message");

// Activate publish client only
        eventHubClientAsync.registerPublishCallback(pubCallback);
        eventHubClientAsync.flush();
        pause(1000L);
        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertTrue(eventHubClientAsync.subscribeClient.isStreamClosed());

// Reconnect and make sure only publish is restarted
        eventHubClientAsync.forceRenewToken();
        pause(1000L);
        assertEquals(0, pubCallback.getErrorCount()); // There should be no error since the reconnect error was expected
        assertEquals(0, subCallback.getErrorCount());
        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertTrue(eventHubClientAsync.subscribeClient.isStreamClosed());

// Activate subscribe
        eventHubClientAsync.subscribe(subCallback);
        assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());
        pause(1000L);
// Reconnect and make sure both are restarting
        pubCallback.resetCounts();
        eventHubClientAsync.forceRenewToken();
        pause(100L);
        assertEquals(0, pubCallback.getErrorCount());
        assertEquals(0, subCallback.getErrorCount());
        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());

// Shutdown a publish client (aka make inactive)
        eventHubClientAsync.publishClient.shutdown("test - reconnectOnlyActiveClients");
        assertTrue(eventHubClientAsync.publishClient.isStreamClosed());
        pause(100L);

// Reconnect and make sure only subscribe reconnects
        pubCallback.resetCounts();
        subCallback.resetCounts();
        eventHubClientAsync.forceRenewToken();
        pause(100L);
        assertEquals(0, pubCallback.getErrorCount());
        assertEquals(0, subCallback.getErrorCount());
        assertTrue(eventHubClientAsync.publishClient.isStreamClosed());
        assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());
    }

    @Test
// UNKNOWN error should be limited to number of future reconnects
    public void status_UNKNOWN_badToken() throws EventHubClientException {
        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .automaticTokenRenew(false)
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .build();

        Client client = new Client(configuration);

        PublishCallback pubCallback = new PublishCallback();
        client.registerPublishCallback(pubCallback);
        TestSubscribeCallback subCallback = new TestSubscribeCallback("");

        client.setAuthToken("R2D2 trying to access..."); // aka bad token .. will get UNKNOWN which will limit reconnects
        client.flush(); // trigger publish
        client.subscribe(subCallback);

// Should start at timeout index 3, 5 delays (in case test fails and goes over 4 delays)
// Same delay for both clients
        pause(BackOffDelayTest.calculatePause(client.publishClient.backOffDelay, 3, 1) * 5);

        assertTrue(client.publishClient.isStreamClosed());
        assertTrue(client.subscribeClient.isStreamClosed());
        assertEquals(5, pubCallback.getErrorCount()); // 1 UNKNOWN original error + 3 UNKNOWN retry errors + 1 reconnect error
        assertEquals(5, subCallback.getErrorCount());

        client.shutdown();
    }

    @Test
// After CANCELLED, there should be no reconnect. CANCELLED trigger by shutdown for publish and unsubscribe for subscribe
    public void status_CANCELLED_userCancelled() throws EventHubClientException {
        PublishCallback pubCallback = new PublishCallback();
        TestSubscribeCallback subCallback = new TestSubscribeCallback("");
        eventHubClientAsync.registerPublishCallback(pubCallback);

// Start both publish and subscribe
        eventHubClientAsync.flush();
        eventHubClientAsync.subscribe(subCallback);
        pause(1000L);
        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());

// Close subscribe
        eventHubClientAsync.unsubscribe();
        pause(BackOffDelayTest.calculatePause(eventHubClientAsync.subscribeClient.backOffDelay, 0, 3)); // pause to make sure no reconnect happens
        assertEquals(0, subCallback.getErrorCount()); // no error since the shutdown was expected
        subCallback.resetCounts();

// Close publish (this also closes everything)
        eventHubClientAsync.shutdown();
        pause(BackOffDelayTest.calculatePause(eventHubClientAsync.publishClient.backOffDelay, 0, 3)); // pause to make sure no reconnect happens
        assertEquals(0, pubCallback.getErrorCount()); // no error since the error was expected
        assertEquals(0, subCallback.getErrorCount()); // make sure nothing triggered here
    }

    @Test
    public void status_UNAVAILABLE_badHost() throws EventHubClientException {
        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .host("ksdksdfk")
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .build();
        Client client = new Client(configuration);

        PublishCallback pubCallback = new PublishCallback();
        TestSubscribeCallback subCallback = new TestSubscribeCallback("no message");
        client.registerPublishCallback(pubCallback);

// Start clients
        client.flush();
        client.subscribe(subCallback);

// Wait for a couple reconnects
        int attempts = 4;
        pause(BackOffDelayTest.calculatePause(client.publishClient.backOffDelay, 0, attempts));

// CHeck and make sure reconnect attempted
        assertThat(pubCallback.getErrorCount(), greaterThanOrEqualTo(attempts));
        assertThat(subCallback.getErrorCount(), greaterThanOrEqualTo(attempts));

        client.shutdown();
    }

    @Test
    public void setReconnectRetryLimit() throws EventHubClientException {
        int reconnectRetryLimit = 0;

        EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .reconnectRetryLimit(reconnectRetryLimit)
                .zoneID("wrongZone")
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .build();
        Client client = new Client(configuration);

        PublishCallback pubCallback = new PublishCallback();
        client.registerPublishCallback(pubCallback);
        client.addMessage("id", "body", null);
        client.flush();

        assertEquals(reconnectRetryLimit, client.publishClient.backOffDelay.retryLimit.get());
        assertEquals(reconnectRetryLimit, client.subscribeClient.backOffDelay.retryLimit.get());

        client.shutdown();
    }

    // This is for the Status.Codes that will reconnect indefinitely
// Manually call error for (length of delay array + 1) to make sure wrap around for delay works
    public void fakeError(Client client, Status.Code code) throws EventHubClientException {
        PublishCallback pubCallback = new PublishCallback();
        TestSubscribeCallback subCallback = new TestSubscribeCallback( "no message");
        client.registerPublishCallback(pubCallback);

// Start both publish and subscribe
        client.flush();
        client.subscribe(subCallback);

        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());

        for (int i = 0; i < BackOffDelayTest.MockBackOffDelay.delayMilliseconds.length + 1; i++) {
// call error
            pause(1000L);
            client.publishClient.throwErrorToStream(code, "", null);
            client.subscribeClient.throwErrorToStream(code, "", null);

// wait for timeout
            pause(BackOffDelayTest.calculatePause(client.publishClient.backOffDelay, i, 2)); // pause to make sure reconnect happens

// make sure reconnect happened (1 for `status` + 1 for CANCELLED)
            assertEquals(2, pubCallback.getErrorCount());
            assertEquals(2, subCallback.getErrorCount());
            assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
            assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());

// clear errors for next loop
            pubCallback.resetCounts();
            subCallback.resetCounts();
        }

        System.out.println("Reconnect " + BackOffDelayTest.MockBackOffDelay.delayMilliseconds.length + 1 + " times");
    }

    @Test
// Check reconnect for UNAVAILABLE
    public void status_UNAVAILABLE_forced() throws EventHubClientException {
        fakeError(eventHubClientAsync, Status.Code.UNAVAILABLE);
    }

    @Test
// Check reconnect for INTERNAL
    public void status_INTERNAL_forced() throws EventHubClientException {
        fakeError(eventHubClientAsync, Status.Code.INTERNAL);
    }

    @Test
// Check reconnect for UNAUTHENTICATED
    public void status_UNAUTHENTICATED_forced() throws EventHubClientException {
        fakeError(eventHubClientAsync, Status.Code.UNAUTHENTICATED);
    }

    @Test
    public void resetCounterAfterReconnect() throws EventHubClientException {
        PublishCallback pubCallback = new PublishCallback();
        eventHubClientAsync.registerPublishCallback(pubCallback);
        eventHubClientAsync.flush();
// Manually force reconnect but make the timeout very long
        Status status = io.grpc.Status.fromCode(Status.Code.UNAVAILABLE).withDescription(null).withCause(new Throwable("java.io.IOException: Connection reset by peer"));
        pubCallback.resetCounts();
        eventHubClientAsync.publishClient.backOffDelay.attempt.set(4);  // Should be 3000ms or 3s in MockBackOffDelay
        eventHubClientAsync.publishClient.streamObserver.onError(new StatusException(status));   // throw an error that will cause the SDK to reconnect
        pause(200L);

// Make sure that reconnect is happening
        assertEquals(2, pubCallback.getErrorCount()); // There should be two error count for the UNAVAILABLE and then CANCELLED
        pubCallback.resetCounts();
        assertTrue(eventHubClientAsync.publishClient.isStreamClosed());

        pause(3500L);

        assertTrue(!eventHubClientAsync.publishClient.isStreamClosed());
        assertEquals(0, eventHubClientAsync.publishClient.backOffDelay.attempt.get());
        assertEquals(0, pubCallback.getErrorCount());
    }

    /**
     * Test for auto close on client
     * outside client here is used to keep a reference to the client
     * outside the try catch so we can make sure the streams are closed
     */
    @Test
    public void testAutoCloseable() {
        String messageBody = createRandomString();
        TestSubscribeCallback callback = new TestSubscribeCallback( messageBody);
        Exception expected = new Exception("Close the channels");
        Client outsideClient = null;
        try (Client c = new Client(eventHubConfigurationSync)) {
            outsideClient = c;
            c.subscribe(callback);
            c.addMessage("id", messageBody, null).flush();
            throw expected;
        } catch (Exception e) {
            assertEquals(e, expected);
        }
        assertNotNull(outsideClient);
        assertTrue(outsideClient.publishClient.isStreamClosed());
        assertTrue(outsideClient.subscribeClient.isStreamClosed());

    }

    @Test
    public void testScopes() throws EventHubClientException {
        String topicSuffix1 = "NewTopic";
        String topicSuffix2 = "NewTopic1";
        List<String> topicSuffixes = new ArrayList<String>();
        topicSuffixes.add(topicSuffix1);
        topicSuffixes.add(topicSuffix2);

        EventHubConfiguration onlyDefaultTopicPublishSubscribe = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).timeout(3000).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberName("onlyDefault")
                        .subscriberInstance("scope-test")
                        .build())
                .build();

        Client onlyDefaultTopicPublishSubscribeClient = new Client(onlyDefaultTopicPublishSubscribe);
        onlyDefaultTopicPublishSubscribeClient.forceRenewToken();


        String scopes[] = {
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".user",
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".NewTopic.grpc.subscribe",
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".NewTopic.grpc.publish"};

        StringBuilder scopeStrings = new StringBuilder();
        for (String s : scopes) {
            scopeStrings.append(s).append(" ");
        }
        EventHubConfiguration onlySubtopicConfig = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .authScopes(scopeStrings.toString())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.ASYNC).build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscriberInstance("sub-instance")
                        .build())
                .build();

        Client onlySubtopicSubscribeClient = new Client(onlySubtopicConfig);
        onlySubtopicSubscribeClient.subscribe(new TestSubscribeCallback( ""));
        onlySubtopicSubscribeClient.addMessage("id", "onlySubtopic", null).flush();
        onlySubtopicSubscribeClient.registerPublishCallback(new PublishCallback());

    }
    /**
     *  This test assumes "NewTopic1" has been created in docker instance, but "NewTopic" has not
     */
    @Test
    public void subToTopicExistsAndOtherDNE() throws EventHubClientException {
        String topic = "NewTopic";
        String topic1 = "NewTopic1";
        String message = "THIS IS A MESSAGE";

        String scopes[] = {
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".user",
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".NewTopic.grpc.subscribe",
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".NewTopic1.grpc.subscribe"};

        StringBuilder scopeStrings = new StringBuilder();
        for (String s : scopes) {
            scopeStrings.append(s).append(" ");
        }

        //"NewTopic1" exists, but "NewTopic" does not
        List<String> multipleTopics = new ArrayList<String>();
        multipleTopics.add(topic);
        multipleTopics.add(topic1);

        EventHubConfiguration multiTopicConfig = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .authScopes(scopeStrings.toString())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .topics(multipleTopics)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .build())
                .build();

        TestSubscribeCallback multiSubCallback = new TestSubscribeCallback(message);
        Client multiSubClient = new Client(multiTopicConfig);
        multiSubClient.subscribe(multiSubCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        //Since one of the topics that subscribed to DNE, callback should have 5 unknown errors
        assertEquals(5, multiSubCallback.getErrorCount());
        multiSubClient.shutdown();
    }

    /**
     *  Tests publish/subscribe to topic that does not exist
     */
    @Test
    public void topicDNE() throws EventHubClientException {
        String topic = "NewTopic";
        String message = "THIS TOPIC DNE";

        String scopes[] = {
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".user",
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".NewTopic.grpc.subscribe",
                "predix-event-hub.zones."+eventHubConfigurationSync.getZoneID()+".NewTopic.grpc.publish"};

        StringBuilder scopeStrings = new StringBuilder();
        for (String s : scopes) {
            scopeStrings.append(s).append(" ");
        }

        EventHubConfiguration publisher = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .authScopes(scopeStrings.toString())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC)
                        .topic(topic)
                        .build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .topic(topic)
                        .build())
                .build();

        Client theClient = new Client(publisher);
        TestSubscribeCallback subscribeCallback = new TestSubscribeCallback(message);
        theClient.forceRenewToken();

        theClient.subscribe(subscribeCallback);

        EventHubClientException.SyncPublisherFlushException syncPublisherFlushException = null;
        try {
            buildAndSendMessages(theClient, message, 1, "DNE");
        } catch(EventHubClientException.SyncPublisherFlushException e) {
            syncPublisherFlushException = e;
        }
        //Topic does not exist
        assertNotNull(syncPublisherFlushException);
        assertEquals(1, syncPublisherFlushException.getThrowables().size());
        assertEquals(5, subscribeCallback.getErrorCount());
        theClient.shutdown();
    }

    /**
     * Test for multiple topics, use two sub topics and the default topic
     * Use three publishers, one for each topic
     * Two subscribers, one for the default and one for the two sub topics
     * Publish 100 messages to each topic, make sure they all come back
     *
     * ASSUMPTION: "NewTopic1" and "NewTopic2" HAVE BEEN CREATED IN DOCKER
     */
    @Test
    public void multipleTopicsTest() throws EventHubClientException {
        //Provide the names of the topic.
        String topicSuffix1 = "NewTopic1";
        String topicSuffix2 = "NewTopic2";


        List<String> topicSuffixes = new ArrayList<String>();
        topicSuffixes.add(topicSuffix1);
        topicSuffixes.add(topicSuffix2);

        EventHubConfiguration configuration_all_topics_subscribe = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .topics(topicSuffixes) //Note the topics(List) vs topic(String)
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .subscriberName(subscriberName + "-1")
                        .subscriberInstance(subscriberID)
                        .build())
                .build();

        EventHubConfiguration configuration_default_topic_subscribe = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .subscribeConfiguration(new SubscribeConfiguration.Builder()
                        .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                        .subscriberName(subscriberName + "-2")
                        .subscriberInstance(subscriberID)
                        .build())
                .build();

        EventHubConfiguration configuration_publish_default = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC)
                        .timeout(2000)
                        .build())
                .build();

        EventHubConfiguration configuration_publish_topic_1 = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC)
                        .topic(topicSuffix1)
                        .build())
                .build();

        EventHubConfiguration configuration_publish_topic_2 = new EventHubConfiguration.Builder()
                .fromEnvironmentVariables()
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC)
                        .topic(topicSuffix2)
                        .build())
                .build();

        Client subscribeMultipleTopics = new Client(configuration_all_topics_subscribe);
        Client subscribeDefault = new Client(configuration_default_topic_subscribe);

        Client publishTopic1 = new Client(configuration_publish_topic_1);
        Client publishTopic2 = new Client(configuration_publish_topic_2);
        Client publishDefault = new Client(configuration_publish_default);

        String messageBody = createRandomString();
        int numMessagePerTopic = 100;
        TestSubscribeCallback defaultCallback = new TestSubscribeCallback(messageBody);
        TestSubscribeCallback multipleTopicCallback = new TestSubscribeCallback(messageBody);
        subscribeDefault.subscribe(defaultCallback);
        subscribeMultipleTopics.subscribe(multipleTopicCallback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);

        buildAndSendMessages(publishDefault, messageBody, numMessagePerTopic, "default");
        buildAndSendMessages(publishTopic1, messageBody, numMessagePerTopic, "topic1");
        buildAndSendMessages(publishTopic2, messageBody, numMessagePerTopic, "topic2");

        multipleTopicCallback.block(numMessagePerTopic * 2);
        defaultCallback.block(numMessagePerTopic);

        assertEquals( numMessagePerTopic * 2, multipleTopicCallback.getMessageCount());
        assertEquals(numMessagePerTopic, defaultCallback.getMessageCount());

        subscribeMultipleTopics.shutdown();
        subscribeDefault.shutdown();
        publishTopic1.shutdown();
        publishTopic2.shutdown();
        publishDefault.shutdown();
    }

    /**
     * This test was written to simulate what happens when the service crashes
     */
    @Test
    @Ignore
    public void extendedTest() throws EventHubClientException, InterruptedException {
        final String randomMessage = createRandomString();
        eventHubClientSync.subscribe(new TestSubscribeCallback(randomMessage));
        Thread publishThread = new Thread() {
            public void run() {
            for(int i=0;i<120;i++) {
                try {
                    eventHubClientSync.addMessage("id", randomMessage, null).flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                pause(500L);
            }
            }
        };
        publishThread.start();
        publishThread.join();

    }

    @Test
    @Ignore
    public void basicEndToEndWithExplicit() throws EventHubClientException {
        EventHubConfiguration configManual = new EventHubConfiguration.Builder()
                .host(System.getenv("EVENTHUB_URI"))
                .port(Integer.parseInt(System.getenv("EVENTHUB_PORT")))
                .zoneID(System.getenv("ZONE_ID"))
                .clientID(System.getenv("CLIENT_ID"))
                .clientSecret(System.getenv("CLIENT_SECRET"))
                .authURL(System.getenv("AUTH_URL"))
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .publishConfiguration(new PublishConfiguration.Builder().publisherType(PublishConfiguration.PublisherType.SYNC).build())
                .build();

        Client c = new Client(configManual);
        String randomMessage = createRandomString();
        TestSubscribeCallback callback = new TestSubscribeCallback(randomMessage);
        c.subscribe(callback);
        pause(SUBSCRIBER_ACTIVE_WAIT_LENGTH);
        buildAndSendMessages(c, randomMessage, 10);
        callback.block(10);
        assertEquals(10, callback.getMessageCount());
        c.shutdown();
    }

    /**
     * Test for the public reconnect method
     * Throw erros the streams and then make sure that when reconnect is called
     * the callbacks are still working
     * @throws EventHubClientException
     */
    @Test
    public void testReconnect() throws EventHubClientException {

        String messageBody  = createRandomString();
        TestUtils.TestSubscribeCallback subCallback = new TestUtils.TestSubscribeCallback( messageBody);
        TestUtils.PublishCallback pubCallback = new TestUtils.PublishCallback();

        // Subscribe both async and sync clients
        eventHubClientSync.subscribe(subCallback);
        eventHubClientAsync.registerPublishCallback(pubCallback);
        eventHubClientAsync.subscribe(subCallback);
        //wait for the subscribers to become active
        pause(10000L);
        //add a message and flush it
        eventHubClientAsync.addMessage("id", messageBody, null).addMessage("id", messageBody, null).flush();
        subCallback.block(4);
        pause(1000L);
        //make sure the streams are working
        assertEquals(4, subCallback.getMessageCount());
        assertEquals(2, pubCallback.getAckCount());
        //throw errors to the stream to close them
        eventHubClientSync.subscribeClient.throwErrorToStream( Status.Code.UNKNOWN, "", null);
        eventHubClientAsync.publishClient.throwErrorToStream(Status.Code.UNKNOWN, "", null);
        assertTrue(eventHubClientSync.subscribeClient.isStreamClosed());
        assertTrue(eventHubClientAsync.publishClient.isStreamClosed());
        //call the reconnect method that will rebuild the clients
        eventHubClientAsync.reconnect();
        eventHubClientSync.reconnect();
        pubCallback.resetCounts();
        subCallback.resetCounts();
        pause(10000L); //wait for clients to become active again

        eventHubClientAsync.addMessage("id", messageBody, null).addMessage("id2", messageBody, null).flush();
        subCallback.block(4);
        pubCallback.block(2);
        assertEquals(2, pubCallback.getAckCount());
        assertEquals(4, subCallback.getMessageCount());


        assertTrue(!eventHubClientSync.subscribeClient.isStreamClosed());
        assertTrue(!eventHubClientAsync.subscribeClient.isStreamClosed());

    }



    @After
    public void shutdown() {
       eventHubClientSync.shutdown();
       eventHubClientAsync.shutdown();
    }
}
