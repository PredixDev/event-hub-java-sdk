/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub.client;

import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.INSTANCE_NAME;
import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.UAA_INSTANCE_NAME;
import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.VCAP_SERVICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.configuration.LoggerConfiguration;
import com.ge.predix.eventhub.test.categories.SanityTest;
import com.ge.predix.eventhub.test.categories.SmokeTest;
import com.google.protobuf.ByteString;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EventHubUtilsTest {

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("################## Starting test ###############: " + description.getMethodName());
        }
    };

    @Test
    @Category(SanityTest.class)
    public void parseVCAPSForEventHubDetails() {
        String VCAPS = System.getenv(VCAP_SERVICES);
        String eventHubInstanceName = System.getenv(INSTANCE_NAME);

        try {
            EventHubConfiguration varsFromVCAPS = EventHubUtils.getEventHubDetailsFromVCAPS(VCAPS, eventHubInstanceName);
            assertEquals(System.getenv("EVENTHUB_URI"), varsFromVCAPS.getHost());
            assertEquals(Integer.parseInt(System.getenv("EVENTHUB_PORT")), varsFromVCAPS.getPort());
            assertEquals(System.getenv("ZONE_ID"), varsFromVCAPS.getZoneID());
        } catch (EventHubClientException.InvalidConfigurationException e) {
            fail();
        }
    }

    @Test
    @Category(SanityTest.class)
    public void parseVCAPSForUAADetails() {
        String VCAPS = System.getenv(VCAP_SERVICES);
        String uaaInstanceName = System.getenv(UAA_INSTANCE_NAME);

        try {
            String UaaURI = EventHubUtils.getUAADetailsFromVCAPS(VCAPS, uaaInstanceName);
            assertEquals(System.getenv("AUTH_URL"), UaaURI);
        } catch (EventHubClientException.InvalidConfigurationException e) {
            fail();
        }
    }

    @Test
    @Category(SanityTest.class)
    public void EventHubConfigFromEnv() {
        try {
// test manually building vs getting from VCAPS
            EventHubConfiguration configFromEnv = new EventHubConfiguration.Builder().fromEnvironmentVariables().build();
            EventHubConfiguration configManual = new EventHubConfiguration.Builder()
                    .host(System.getenv("EVENTHUB_URI"))
                    .port(Integer.parseInt(System.getenv("EVENTHUB_PORT")))
                    .zoneID(System.getenv("ZONE_ID"))
                    .clientID(System.getenv("CLIENT_ID"))
                    .clientSecret(System.getenv("CLIENT_SECRET"))
                    .authURL(System.getenv("AUTH_URL"))
                    .build();

            assertEquals(configFromEnv, configManual);

// test giving the event hub and uaa instance names to find in VCAP_SERVICES
            final String eventHubInstanceName = System.getenv(INSTANCE_NAME);
            final String uaaInstanceName = System.getenv(UAA_INSTANCE_NAME);
            EventHubConfiguration configGiven = new EventHubConfiguration.Builder().fromEnvironmentVariables(eventHubInstanceName, uaaInstanceName).build();

            assertEquals(configFromEnv, configGiven);

// test giving only the event hub instance name to find in VCAP_SERVICES
            EventHubConfiguration configEventHubOnly = new EventHubConfiguration.Builder().fromEnvironmentVariablesWithInstance(eventHubInstanceName).automaticTokenRenew(false).build();
            EventHubConfiguration configEventHubOnlyManual = new EventHubConfiguration.Builder()
                    .host(System.getenv("EVENTHUB_URI"))
                    .port(Integer.parseInt(System.getenv("EVENTHUB_PORT")))
                    .zoneID(System.getenv("ZONE_ID"))
                    .automaticTokenRenew(false)
                    .build();

            assertEquals(configEventHubOnly, configEventHubOnlyManual);

            EventHubConfiguration configAuthURLOnly = new EventHubConfiguration.Builder().fromEnvironmentVariables(System.getenv("AUTH_URL")).build();

            assertEquals(configFromEnv, configAuthURLOnly);

        } catch (EventHubClientException.InvalidConfigurationException e) {
            fail();
        }
    }

    @Test
    @Category(SanityTest.class)
    public void badEventHubConfig() {
        boolean caught = false;
        try {
            EventHubConfiguration badConfig = new EventHubConfiguration.Builder().host("some host").build();
        } catch (EventHubClientException.InvalidConfigurationException e) {
            caught = true;
        }
        assertTrue(caught); // should catch that values are not set
    }

    @Test
    @Category(SanityTest.class)
    public void badVCAPS() {
        int caught = 0;

// no VCAPS
        try {
            EventHubUtils.getEventHubDetailsFromVCAPS(null, "some instance name");
        } catch (EventHubClientException.InvalidConfigurationException e) {
            caught++;
        }

// instance not in VCAPS
        try {
            EventHubUtils.getEventHubDetailsFromVCAPS("{\"VCAP_SERVICES\":{\"service name\":[{\"name\":\"some instance name\"}]}}",
                    "some bad instance name");
        } catch (EventHubClientException.InvalidConfigurationException e) {
            caught++;
        }

// VCAPS structure is missing grpc protocol
        try {
            EventHubUtils.getEventHubDetailsFromVCAPS(
                    "{\"VCAP_SERVICES\":{\"service name\":[{\"name\":\"some instance name\", \"credentials\":{\"publish\":{\"protocol_details\":[{\"protocol\":\"bad protocal\"}]}}}]}}",
                    "some instance name");
        } catch (EventHubClientException.InvalidConfigurationException e) {
            caught++;
        }

        assertEquals(3, caught);
    }

    @Test
    @Category(SanityTest.class)
    public void instanceFromVCAPSSingleton() {
        String VCAPS = System.getenv(VCAP_SERVICES);
        String eventHubInstanceName = System.getenv(INSTANCE_NAME);

        try {
// pull instance details once and after that the details should come from cache
            EventHubConfiguration varsFromVCAPS = EventHubUtils.getEventHubDetailsFromVCAPS(VCAPS, eventHubInstanceName);
            EventHubConfiguration varsFromHashMap = EventHubUtils.getEventHubDetailsFromVCAPS("junk", eventHubInstanceName);
            assertEquals(varsFromVCAPS, varsFromHashMap);
        } catch (EventHubClientException.InvalidConfigurationException e) {
            System.out.println(e.getMessage());
            fail();
        }
    }

    @Test
    @Category(SanityTest.class)
    public void instanceFromVCAPSThreading() throws Exception {
        final String VCAPS = System.getenv(VCAP_SERVICES);
        final String eventHubInstanceName = System.getenv(INSTANCE_NAME);
        final String uaaInstanceName = System.getenv(UAA_INSTANCE_NAME);

        final Map<String, JsonNode> map = new ConcurrentHashMap<String, JsonNode>();
        final CountDownLatch startSync = new CountDownLatch(1);

        Thread eventHubThread = new Thread() {
            public void run() {

                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    map.put(eventHubInstanceName, EventHubUtils.findByServiceName(VCAPS, eventHubInstanceName));
                } catch (EventHubClientException.InvalidConfigurationException e) {
                    fail();
                }
            }
        };

        Thread uaaThread = new Thread() {
            public void run() {

                try {
                    startSync.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    map.put(uaaInstanceName, EventHubUtils.findByServiceName(VCAPS, uaaInstanceName));
                } catch (EventHubClientException.InvalidConfigurationException e) {
                    fail();
                }
            }
        };

        eventHubThread.start();
        uaaThread.start();
        startSync.countDown();
        eventHubThread.join();
        uaaThread.join();

        assertNotEquals(map.get(eventHubInstanceName), map.get(uaaInstanceName));
        assertEquals(eventHubInstanceName, map.get(eventHubInstanceName).get("name").asText());
        assertEquals(uaaInstanceName, map.get(uaaInstanceName).get("name").asText());
    }

    /**
     * test that tests all the different log json generator functions
     *
     */
    @Test
    @Category(SanityTest.class)
    public void testJSONMaker(){
        Ack a = Ack.newBuilder()
                .setId("ack-id")
                .setOffset(2)
                .setPartition(10)
                .setStatusCode(AckStatus.ACCEPTED)
                .setZoneId("z-o-n-e--i-d")
                .build();

        EventHubUtils.makeJson(a);


        List<Message> messagesList = new ArrayList();
        for(int i=0; i<10;i++){
            messagesList.add(Message.newBuilder()
                    .setId("mesasge-id-" + i)
                    .setBody(ByteString.copyFromUtf8("message-body"))
                    .setZoneId("zone-id")
                    .build()
            );

        }
        PublishResponse publishResponse = PublishResponse.newBuilder()
                .addAck(a)
                .addAck(a)
                .addAck(a)
                .build();

        EventHubUtils.makeJson(messagesList);

        Throwable t = new Exception("I AM A EXCEPTION!!!");

        EventHubUtils.formatJson("test-this",
                "ack", a,
                "messages", messagesList,
                "throwable", t,
                "publishResponse", publishResponse,
                "innerJson", EventHubUtils.formatJson(
                        "single message", messagesList.get(0)
                ));

    }

    /**
     * test to cycle though all the log levels
     * there is no real way to assure they functioned properly
     * but it will at least run the code
     */
    @Test
    @Category(SanityTest.class)
    public void testLogLevels(){
        LoggerConfiguration utilLoggingConfig = new LoggerConfiguration.Builder()
                .logLevel(LoggerConfiguration.LogLevel.ALL)
                .prettyPrintJsonLogs(true)
                .useJUL(true)
                .build();
        LoggerConfiguration slf4jLoggingConfig = new LoggerConfiguration.Builder()
                .logLevel(LoggerConfiguration.LogLevel.ALL)
                .prettyPrintJsonLogs(true)
                .build();

        EventHubLogger utilLogger = new EventHubLogger(this.getClass(), utilLoggingConfig);
        EventHubLogger slf4jLogger = new EventHubLogger(this.getClass(), slf4jLoggingConfig);

        for(Level level : new Level[] {Level.OFF, Level.WARNING, Level.SEVERE, Level.FINE, Level.FINER, Level.FINEST, Level.INFO, Level.ALL}){
            utilLogger.log(level, "log level", level.toString());
            slf4jLogger.log(level, "log level", level.toString());
        }
    }

}
