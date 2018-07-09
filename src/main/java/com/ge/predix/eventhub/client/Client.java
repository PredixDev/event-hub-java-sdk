/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub.client;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.*;
import javax.net.ssl.SSLException;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.configuration.LoggerConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.ge.predix.eventhub.stub.Ack;
import com.ge.predix.eventhub.stub.Message;
import com.ge.predix.eventhub.stub.Messages;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import org.json.JSONObject;

import static com.ge.predix.eventhub.EventHubConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.ClientConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.*;

/**
 * Event hub client object
 */
public class Client implements AutoCloseable {
    private static final String healthCheckUrl = "predix-event-hub.grpc.health";
    private static final Long ttlHealth = 30000L;
    private static final String sdkVersionString = "2.0.7";


    private Channel channel;
    private ManagedChannel originChannel;
    protected HeaderClientInterceptor interceptor;
    private Timer healthCheckTimer;
    private Thread healthThread;
    private CountDownLatch timerStart;

    protected  EventHubLogger ehLogger;
    protected PublishClient publishClient;
    protected SubscribeClient subscribeClient;
    protected EventHubConfiguration configuration;
    private AtomicBoolean shutdownHasOccurred = new AtomicBoolean(false);

    private boolean prettyPrintJson = false;
    /**
     * Required by Autoclosable
     * Call the shutdown method of the Client
     * @throws Exception
     */
    public void close() throws Exception {
        this.shutdown();
    }

    /**
     * Interface for publish callback
     */
    public interface PublishCallback {
        void onAck(List<Ack> acks);

        void onFailure(Throwable throwable);
    }

    /**
     * Parent Interface for both subscriber callbacks
     * You must implement either the SubscribeCallback or the SubscribeBatchCallback
     */
    interface Callback{
        void onMessage(Object o);
        void onFailure(Throwable throwable);
    }

    public interface SubscribeCallback extends Callback{
        default void onMessage(Object o){
            onMessage((Message) o);
        }
        void onMessage(Message m);
        void onFailure(Throwable throwable);
    }
    /**
     * Interface for subscribe callback
     */
    public interface SubscribeBatchCallback extends Callback{
        default void onMessage(Object o){
            onMessage((List<Message>) o);
        }
        void onMessage(List<Message> messages);
        void onFailure(Throwable throwable);
    }

    /**
     * Creates a new client from the given configuration
     *
     * @param configuration Configuration that has Event Hub details and preferences
     */
    public Client(EventHubConfiguration configuration) {
        this.ehLogger = new EventHubLogger(this.getClass(), configuration);
        this.configuration = configuration;
        ehLogger.log(Level.INFO, "starting EventHub client");

        JSONObject mandatoryClientLog = new JSONObject();
        mandatoryClientLog.put("sdk_version", sdkVersionString);
        mandatoryClientLog.put("zoneID", this.configuration.getZoneID());
        mandatoryClientLog.put("runtimeID", this.configuration.getLoggerConfiguration().getRuntimeId());
        System.out.println(mandatoryClientLog);

        buildChannel();
        startHealthChecker();
        initSubscribeClient();
        initPublishClient();

    }

    /**
     * Build the channel for publish and/or subscribe
     */
    private void buildChannel() {
        this.interceptor = new HeaderClientInterceptor(this, this.configuration);

        ehLogger.log( Level.FINE,
                CLIENT_CHANNEL_MSG,
                MSG_KEY, "building channel",
                HOST_STRING, this.configuration.getHost(),
                PORT_STRING, this.configuration.getPort());

//grab pem from local env
        try {
            String pemFileLoc = System.getenv("TLS_PEM_FILE");
            if ( pemFileLoc == null) {
                originChannel = NettyChannelBuilder.forAddress(configuration.getHost(), configuration.getPort())
                        .sslContext(GrpcSslContexts.forClient().build()).build();
            } else {
                if(pemFileLoc.equals("none")){
                    originChannel = NettyChannelBuilder.forAddress(configuration.getHost(), configuration.getPort()).usePlaintext(true).build();
                }else {
                    originChannel = NettyChannelBuilder.forAddress(configuration.getHost(), configuration.getPort()).sslContext(GrpcSslContexts.forClient()
                            .trustManager(new File(System.getenv("TLS_PEM_FILE"))).build()).build();
                }
            }
            this.channel = ClientInterceptors.intercept(originChannel, interceptor);
            ehLogger.log( Level.INFO,
                    CLIENT_CHANNEL_MSG,
                    MSG_KEY, "transport channel built");

        } catch (SSLException e) {
            ehLogger.log( Level.SEVERE,
                    CLIENT_CHANNEL_ERROR,
                    MSG_KEY, "could not build channel",
                    HOST_STRING, this.configuration.getHost(),
                    PORT_STRING, this.configuration.getPort(),
                    EXCEPTION_KEY, e
            );
        }


    }

    /**
     * Health Checker thread,
     * Create a new Stub that will periodically check() the connection
     * The observer does nothing onNext(), log an error onError()
     */
    private void startHealthChecker() {
        ehLogger.log( Level.INFO,
                CLIENT_MSG,
                MSG_KEY, "starting health checker");
        final HealthGrpc.HealthStub healthStub = HealthGrpc.newStub(originChannel);
        final HealthCheckRequest request = HealthCheckRequest.newBuilder().setService(healthCheckUrl).build();
        final StreamObserver<HealthCheckResponse> observer = new StreamObserver<HealthCheckResponse>() {
            public void onNext(HealthCheckResponse healthCheckResponse) {
                ehLogger.log( Level.FINEST,
                        CLIENT_CHANNEL_MSG,
                        MSG_KEY, "received health check response");
            }

            public void onError(Throwable throwable) {
                ehLogger.log( Level.WARNING,
                        CLIENT_CHANNEL_ERROR,
                         MSG_KEY, "error in health check",
                         EXCEPTION_KEY,throwable
                        );
            }

            public void onCompleted() {
                ehLogger.log( Level.FINE,
                        CLIENT_CHANNEL_MSG,
                        MSG_KEY, "health check channel complete");
            }
        };

        final TimerTask checkHealth = new TimerTask() {
            @Override
            public void run() {
                ehLogger.log( Level.FINEST,
                        CLIENT_CHANNEL_MSG,
                        MSG_KEY, "pinging event hub for health check");
                try {
                    healthStub.check(request, observer);
                }
                catch(Exception e){
                    ehLogger.log( Level.SEVERE,
                            CLIENT_CHANNEL_ERROR,
                            MSG_KEY, "exception caught in healthStub.check",
                            FUNCTION_NAME_STRING, "Client.startHealthChecker.TimerTask.run",
                            EXCEPTION_KEY, e
                    );
                }
            }
        };
        // to prevent the race condition of the healthCheckTimer being called before
        // it get created, wait for the health thread to run before exiting the init method
        timerStart = new CountDownLatch(1);

        healthCheckTimer = new Timer();
        healthThread = new Thread() {
            public void run() {
                healthCheckTimer = new Timer();
                healthCheckTimer.schedule(checkHealth, 0L, ttlHealth);
                timerStart.countDown();
            }
        };
        healthThread.start();
        try {
            timerStart.await();
        } catch (InterruptedException e) {
            ehLogger.log(Level.SEVERE,
                    CLIENT_ERROR,
                    MSG_KEY, "error wating for health timer to start",
                    EXCEPTION_KEY, e);
        }
    }

    /**
     * Stop the healthchecker tread
     */
    private void stopHealthChecker() {
        ehLogger.log( Level.INFO,
                CLIENT_MSG,
                FUNCTION_NAME_STRING, "Client.stopHealthChecker",
                MSG_KEY, "stopping health checker");
        healthCheckTimer.cancel();
        try {
            healthThread.join();
        } catch (InterruptedException e) {
            ehLogger.log( Level.WARNING,
                    CLIENT_CHANNEL_ERROR,
                    FUNCTION_NAME_STRING, "Client.stopHealthChecker",
                    MSG_KEY, "health check stop interrupted",
                    EXCEPTION_KEY, e
            );
        }
    }

    /*
    * Because Client() can be publish, subscribe or both and we do not know permissions, we will initialize when used.
    * Need to ensure that the clients are configured before sending or receiving.
    */
    private synchronized void initPublishClient() {
        if (configuration.getPublishConfiguration() != null &&  publishClient == null) {
            if (this.configuration.getPublishConfiguration().getPublishMode() == PublishConfiguration.PublisherType.ASYNC) {
                publishClient = new AsyncPublishClient(this.channel, originChannel, this.configuration);
            } else if (this.configuration.getPublishConfiguration().getPublishMode() == PublishConfiguration.PublisherType.SYNC) {
                publishClient = new SyncPublisherClient(this.channel, originChannel, this.configuration);
            }
        }
    }

    private synchronized void initSubscribeClient() {
        if (configuration.getSubscribeConfiguration() != null &&  subscribeClient == null) {
            if(configuration.getSubscribeConfiguration().isBatchingEnabled()){
                subscribeClient = new BatchSubscribeClient(channel, originChannel, configuration);
            }else if(configuration.getSubscribeConfiguration().isAcksEnabled()){
                subscribeClient = new AckSubscribeClient(channel, originChannel, configuration);
            }else{
                subscribeClient = new StandardSubscribeClient(channel, originChannel, configuration);
            }
        }
    }


    /**
     * send a reconnect to all clients with active streams
     * The clients need to be active which means it has been initialized at least once and has not been shutdown
     *
     * @param cause
     * @throws EventHubClientException if shutdown has been called
     */
     synchronized void reconnectClients(String cause) throws EventHubClientException {
         ehLogger.log( Level.INFO,
                 CLIENT_MSG,
                 MSG_KEY, "reconnecting clients",
                 CAUSE_KEY,cause,
                 FUNCTION_NAME_STRING, "Client.reconnectClients");
        checkIfShutdown();
        if (publishClient != null && publishClient.isClientActive()) {
            publishClient.reconnectStream(cause);
        }

        if (subscribeClient != null && subscribeClient.isClientActive()) {
            subscribeClient.reconnectStream(cause);
        }
    }

    /**
     * User reconnect function, will rebuild the channel
     * and re-register the callbacks if there were any.
     * This does not rebuild the active streams as they will get
     * re created on first use.
     *
     * @throws EventHubClientException
     */
    public synchronized void reconnect() throws EventHubClientException {
        ehLogger.log( Level.INFO,
                CLIENT_MSG,
                FUNCTION_NAME_STRING, "Client.reconnect",
                MSG_KEY, "user issued a reconnect"
        );

        PublishCallback pubCallback = null;
        if (publishClient != null && this.configuration.getPublishConfiguration().isAsyncConfig()) {
            pubCallback = ((AsyncPublishClient) publishClient).getCallback();
        }
        Callback subscribeCallback = null;
        if (subscribeClient != null) {
            try {
                subscribeCallback = subscribeClient.getCallback();
            } catch (EventHubClientException ignore) {
//there was no subscribe callback
            }
        }
        ehLogger.log( Level.FINE,
                CLIENT_MSG,
                FUNCTION_NAME_STRING, "Client.reconnect",
                MSG_KEY, "shutting down client"
        );
        this.shutdown();
        ehLogger.log( Level.FINE,
                CLIENT_MSG,
                FUNCTION_NAME_STRING, "Client.reconnect",
                MSG_KEY, "building origin channel"
        );
        this.buildChannel();
        ehLogger.log( Level.FINE,
                        CLIENT_MSG,
                FUNCTION_NAME_STRING, "Client.reconnect",
                MSG_KEY, "starting health checker"
        );
        this.startHealthChecker();
        this.publishClient = null;
        this.subscribeClient = null;
        initSubscribeClient();
        initPublishClient();
        if (pubCallback != null) {
            ehLogger.log( Level.FINE,
                    CLIENT_MSG,
                    FUNCTION_NAME_STRING, "Client.reconnect",
                    MSG_KEY, "registering publish callback for client after reconnect"
            );
            ((AsyncPublishClient) this.publishClient).registerPublishCallback(pubCallback);
        }
        if (subscribeCallback != null) {
            ehLogger.log( Level.FINE,
                    CLIENT_MSG,
                    FUNCTION_NAME_STRING, "Client.reconnect",
                    MSG_KEY, "registering subscribe callback for client after reconnect"
            );
            this.subscribeClient.subscribe(subscribeCallback);
        }
        this.shutdownHasOccurred.set(false);
    }

    /**
     * Override current token with provided token. This will force a client reconnection
     *
     * @param token Token used to override current token
     */
    public void setAuthToken(String token) throws EventHubClientException {
        this.interceptor.setAuthToken(token, "setAuthToken");
        reconnectClients("from setAuthToken");
    }

    /**
     * Get the current token
     *
     * @return The current token
     */
    public String getAuthToken() {
        return this.interceptor.getAuthToken();
    }

    /**
     * Renews the token from the configured Oauth2 service
     *
     * @throws EventHubClientException Thrown if there are issues getting the token or a bad configuration or if shutdown has been called
     */
    public synchronized void forceRenewToken() throws EventHubClientException {
        checkIfShutdown();
        if (!this.configuration.isAutomaticTokenRenew()) {
            throw new EventHubClientException(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't renew token with provided config",
                    FUNCTION_NAME_STRING, "Client.forceRenewToken").toString());
        }
        this.interceptor.forceRenewToken();
        reconnectClients("forcerenew token");
    }

    /**
     * Use to close down all the connections of the Client
     *
     */
    public synchronized void shutdown() {
        shutdownHasOccurred.set(true);
        if (publishClient != null) {
            publishClient.shutdown("user called shutdown");
        }

        if (subscribeClient != null) {
            subscribeClient.shutdown("user called shutdown");
        }

        this.stopHealthChecker();
        this.originChannel.shutdown();
        ehLogger.log( Level.INFO,
                CLIENT_CHANNEL_MSG,
                MSG_KEY, "channel closed",
                FUNCTION_NAME_STRING, "Client.shutdown");
    }

    /**
     * Adds a message into queue to be sent
     *
     * @param id   The identification to match ack with message
     * @param body The content of the message
     * @param tags Key value map to store data
     * @return The Client instance
     */
    public synchronized Client addMessage(String id, String body, Map<String, String> tags) throws EventHubClientException {
        Message.Builder messageBuilder = Message.newBuilder().setId(id).setZoneId(this.configuration.getZoneID())
                .setBody(ByteString.copyFromUtf8(body));
        if (tags != null) {
            messageBuilder.getMutableTags().putAll(tags);
        }

        return addMessage(messageBuilder.build());
    }

    /**
     * Adds a single message into queue to be sent
     *
     * @throws EventHubClientException if no publish configuration was supplied or if shutdown has been called
     * @throws EventHubClientException.AddMessageException if the message added makes the queue size more than than the configured amount
     * @param message The message to be added
     * @return The Client instance so addMessages can be chained together
     */
    public synchronized Client addMessage(Message message) throws EventHubClientException {
        Messages messages = Messages.newBuilder().addMsg(message).build();
        return addMessages(messages);
    }

    /**
     * Adds all messages in a Messages object into queue to be sent
     *
     * @throws EventHubClientException if no publish configuration was supplied
     * @throws EventHubClientException.AddMessageException if the number of messages added makes the queue size more than than the configured amount
     * @param messages The messages to be added
     * @return The Client instance so addMessages can be chained together
     */
    public synchronized Client addMessages(Messages messages) throws EventHubClientException {
        checkIfShutdown();
        if(publishClient == null){
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "publisher config not supplied",
                    FUNCTION_NAME_STRING, "Client.addMessage",
                    PUBLISHER_CONFIG_STRING, NULL_STRING).toString());
        }
        publishClient.addMessages(messages);
        return this;
    }

    /**
     * Add callback to receive acks and errors while publishing in ASYNC mode
     *
     * @param callback Callback instance of type @see PublishCallback
     * @throws EventHubClientException Thrown if publish mode is SYNC or if no publish configuration was supplied
     */
    public void registerPublishCallback(PublishCallback callback) throws EventHubClientException {
        if (publishClient == null) {
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "publisher config not supplied",
                    FUNCTION_NAME_STRING, "Client.registerCallback",
                    PUBLISHER_CONFIG_STRING, NULL_STRING
            ).toString());
        }else if(configuration.getPublishConfiguration().isAsyncConfig()) {
            ((AsyncPublishClient) publishClient).registerPublishCallback(callback);
        }
        else {
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't register callback of non async publisher",
                    FUNCTION_NAME_STRING, "Client.registerPublisherCallback",
                    PUBLISHER_CONFIG_STRING, configuration.getPublishConfiguration().toString()).toString());
        }
    }

    /**
     * Retrieve publish callback to receive acks and errors while publishing in ASYNC mode
     * @throws EventHubClientException if the the configuration supplied was not an AsyncPublisher
     */
    public PublishCallback getPublishCallback() throws EventHubClientException {

        if (publishClient == null) {
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't get callback when no publisher config was supplied",
                    FUNCTION_NAME_STRING, "Client.getPublisherCallback",
                    PUBLISHER_CONFIG_STRING, NULL_STRING).toString());
        }else if(configuration.getPublishConfiguration().isAsyncConfig()) {
            return ((AsyncPublishClient) publishClient).getCallback();
        }
        else {
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't get callback of non async callback",
                    FUNCTION_NAME_STRING, "Client.getPublisherCallback",
                    PUBLISHER_CONFIG_STRING, configuration.getPublishConfiguration().toString()).toString());
        }

    }

    void throwErrorToStream(String streamName, Status.Code c, String cause, Throwable t){
        if(streamName.contains("Publisher")){
            if(publishClient != null){
                publishClient.throwErrorToStream(c, cause, t);
            }
        }else if(streamName.contains("Subscriber")){
            if(subscribeClient!=null){
                subscribeClient.throwErrorToStream(c, cause, t);
            }

        }
    }

    /**
     * Retrieve subscribe callback if it exists
     *
     * @throws EventHubClientException Thrown if subscribe client has not been initialized
     * @return generic callback object
     */
    public Callback getSubscribeCallback() throws EventHubClientException {
        if(subscribeClient == null){
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't get callback when no subscriber config was supplied",
                    FUNCTION_NAME_STRING, "Client.getSubscribeCallback",
                    SUBSCRIBER_CONFIG_STRING, NULL_STRING).toString());
        }
        return subscribeClient.getCallback();
    }

    /**
     * Sends all the messages which have been already added
     * For Sync this function will be blocking until all the message acks have returned or a timeout has been reached
     *
     * @return For SYNC, the list of acks will identify what was successfully sent to Event Hub
     * For ASYNC, this list will always be empty because the acks are returned in the callback instead
     */
    public synchronized List<Ack> flush() throws EventHubClientException {
        checkIfShutdown();
        if(publishClient == null){
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    INVALID_CONFIG_MESSAGE,
                    FUNCTION_NAME_STRING, "Client.flush",
                    SUBSCRIBER_CONFIG_STRING, NULL_STRING).toString());
        }
        return publishClient.flush();
    }


    /**
     * Assign callback to start receiving messages from Event Hub
     * The client when then build the required stream depending on the configuration supplied on creation
     *
     * @throws EventHubClientException if no subscribe client was provided or if shutdown has been called
     * @throws EventHubClientException.SubscribeCallbackException if incorrect callback type was provided

     * @param callback Callback instance of type @see SubscribeCallback
     */
    public void subscribe(Callback callback) throws EventHubClientException {
        checkIfShutdown();
        if(subscribeClient == null){
            throw new EventHubClientException(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't subscribe when no subscriber config was supplied",
                    FUNCTION_NAME_STRING, "Client.subscribe",
                    SUBSCRIBER_CONFIG_STRING, NULL_STRING).toString());
        }
        subscribeClient.subscribe(callback);
    }


    /**
     * Acknowledge a received message for a subscriber.
     *
     * @throws EventHubClientException if the client has not initialized or is not active or if shutdown has been called
     * @throws EventHubClientException.IncorrectConfiguration if acks where not enabled for the subscription
     * @param message Message to acknowledge
     */
    public void sendAck(Message message) throws EventHubClientException {
        List<Message> messageList = new ArrayList<Message>();
        messageList.add(message);
        sendAcks(messageList);
    }

    /**
     * Acknowledge received messages for a subscriber.
     *
     * @throws EventHubClientException.IncorrectConfiguration if acks are not enabled for the client
     * @throws EventHubClientException if subscribe has not been called before sending acks or if shutdown has been called
     * @param messages List of messages to acknowledge
     */
    public void sendAcks(List<Message> messages) throws EventHubClientException {
        checkIfShutdown();
        if(subscribeClient == null){
            throw new EventHubClientException.InvalidConfigurationException(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't send acks when no subscribe config was supplied",
                    FUNCTION_NAME_STRING, "Client.sendAck(s)",
                    SUBSCRIBER_CONFIG_STRING, NULL_STRING).toString());
        }
        subscribeClient.sendAcks(messages);
    }

    private void checkIfShutdown() throws EventHubClientException{
        if(shutdownHasOccurred.get()){
            throw new EventHubClientException(EventHubUtils.formatJson(CLIENT_ERROR,
                    MSG_KEY, "can't preform action on a shutdown client"
            ).toString());
        }
    }

    /**
     * Stop receiving messages to the callback
     * @throws EventHubClientException if the subscriberClient is not active
     */
    public void unsubscribe() throws EventHubClientException {
        if(subscribeClient == null){
            throw new EventHubClientException.InvalidConfigurationException(EventHubUtils.formatJson(
                    CLIENT_ERROR,
                    MSG_KEY, "can't unsubscribe when no subscribe config was supplied",
                    FUNCTION_NAME_STRING, "Client.unsubscribe",
                    SUBSCRIBER_CONFIG_STRING, NULL_STRING).toString());
        }
        subscribeClient.unsubscribe();
    }

    public String toString(){
        return EventHubUtils.formatJson(
                "version", sdkVersionString,
                "configuration", configuration.toString()
                ).toString();
    }
}
