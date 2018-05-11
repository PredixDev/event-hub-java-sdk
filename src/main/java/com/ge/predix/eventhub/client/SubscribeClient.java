package com.ge.predix.eventhub.client;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.client.Client.Callback;

import static com.ge.predix.eventhub.EventHubConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.SubscribeClientConstants.*;
import static com.ge.predix.eventhub.EventHubUtils.checkErrorAllowed;

/**
 * Abstract subscriber client for managing shared code for AckSubscribeClient, StandardSubscribeClient and BatchSubscribeClient
 */
abstract class SubscribeClient extends ClientInterface {
    protected EventHubConfiguration configuration;
    protected SubscriberGrpc.SubscriberStub stub;
    private Context.CancellableContext context;
    private ManagedChannel originChannel;
    private Callback callback = null;
    protected StreamObserver<Object> parentStreamObserver;
    protected  EventHubLogger ehLogger;
    private Throwable expectedError = null;
    protected BackOffDelay backOffDelay;
    protected boolean activeClient; // Initially false, true only when createStream() is called, false when shutdown is called (NOT closeStream())
    protected Metadata header;

    /**
     * SubscribeClient abstract class, implements most the overhead for creating a subscription
     * but leaves the per subscription implementation to its children
     *
     * @param channel       the grpc originChannel to use for subscription streams
     * @param configuration the configuration to be used to configure the subscriber
     */

    protected SubscribeClient(Channel channel, ManagedChannel originChannel, EventHubConfiguration configuration){
        this.originChannel = originChannel;
        this.ehLogger = new EventHubLogger(this.getClass(), configuration);
        this.backOffDelay = new BackOffDelay(this, ehLogger);
        this.stub = SubscriberGrpc.newStub(channel);
        this.configuration = configuration;
        this.activeClient = false;
        this.header = new Metadata();
        if(configuration.getSubscribeConfiguration().getSubscriberName().equals("default-subscriber-name")){
            ehLogger.log( Level.SEVERE,
                    SUBSCRIBER_MSG,
                    MSG_KEY, "you are using the default subscriber name, please do not use in production"
            );
        }
        setDefaultHeaders();

        if (configuration.getReconnectRetryLimit() != null) {
            ehLogger.log( Level.INFO, "Setting retry limit to %d", configuration.getReconnectRetryLimit());
            backOffDelay.setRetryLimit(configuration.getReconnectRetryLimit());
        }
    }

    /**
     * Each type of subscriber handles acks though their own stream so they must implement this
     * @param messages The messages to be acked
     */
    protected abstract void sendRequiredAcks(List<Message> messages);

    /**
     * Each type of subscriber must set up its stream differently
     * this is called in the createRequiredStream
     */
    protected abstract void setupRequiredRun();

    abstract SubscribeConfiguration.SubscribeStreamType getSubscribeType();


    /**
     * Get the current registered callback
     *
     * @return the current subscribe callback object
     * @throws EventHubClientException
     */
    public Client.Callback getCallback() throws EventHubClientException {
        if (callback != null) {
            return callback;
        }
        throw new EventHubClientException("Subscribe Callback has not been created");
    }

    protected abstract void validateCallback(Callback c) throws EventHubClientException.SubscribeCallbackException;

    /**
     * Build the required stream for the client. Each of the subscribers pass their stream observers
     * to the parent stream observer. This gives a single point where all messages/errors pass though
     * this also could have been a callback since we are only using the interface of the stream observer
     */
    private synchronized void createRequiredStream() {
        this.activeClient = true;
        this.parentStreamObserver = new StreamObserver<Object>() {
            @Override
            public void onNext(Object o) {
                ehLogger.log(Level.FINEST,
                        SUBSCRIBE_STREAM_MSG,
                        FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.onNext",
                        MSG_KEY, "received message",
                        "received", o);
                try {
                    callback.onMessage(o);
                }catch (Exception e){
                    ehLogger.log(Level.SEVERE,
                            SUBSCRIBER_ERR,
                            MSG_KEY, "exception caught during subscriber callback",
                            "SubscribeMessage", o,
                            EXCEPTION_KEY, e);
                }
            }
            @Override
            public void onError(Throwable throwable) {
                if (checkErrorAllowed(throwable, expectedError)) {
                    ehLogger.log( Level.FINE,
                            SUBSCRIBE_STREAM_ERROR,
                            MSG_KEY, "silencing expected error",
                            FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.onError",
                            "expected_error", expectedError,
                            EXCEPTION_KEY,throwable
                    );
                    expectedError = null;
                } else {
                    Status status = Status.fromThrowable(throwable).getCode().toStatus(); // Get only the status
                    // make sure not already cancelled
                    if (context.isCancelled() || status == Status.CANCELLED) {
                        throwError(EventHubUtils.formatJson(
                                SUBSCRIBE_STREAM_ERROR,
                                FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.onError",
                                MSG_KEY, "stream already cancelled or received cancelled status",
                                EXCEPTION_KEY, throwable
                            ).toString());
                    } else {
                        throwError(EventHubUtils.formatJson(
                                SUBSCRIBE_STREAM_ERROR,
                                FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.onError",
                                MSG_KEY, "stream closing",
                                EXCEPTION_KEY, throwable
                        ).toString());
                        closeStream(throwable);
                    }

                    try {
                        // This will handle reconnect (including deciding whether or not to reconnect)
                        // The conditional above ensures that the context will be closed before this
                        if (isClientActive()) {
                            backOffDelay.initiateReconnect(status);
                        }
                    } catch (EventHubClientException.ReconnectFailedException reconnectFailedException) {
                        throwError(reconnectFailedException);
                        closeStream(reconnectFailedException);
                    }
                }
            }
            @Override
            public void onCompleted() {
                ehLogger.log( Level.FINE,
                        SUBSCRIBE_STREAM_MSG,
                        MSG_KEY, "subscribe stream complete, throwing unavaiable to iniate reconenct", FUNCTION_NAME_STRING, "createRequiredStream.SubscribeStream.onCompleted");
                try {
                    backOffDelay.initiateReconnect(Status.UNAVAILABLE);
                } catch (EventHubClientException.ReconnectFailedException e) {
                    throwError(e);
                }
            }
        };
        this.context = Context.current().withCancellation();
        this.context.run(() -> {
            try{
                setupRequiredRun();
                while(originChannel.getState(true).equals(ConnectivityState.CONNECTING)){
                    try {
                        ehLogger.log(Level.FINEST,
                                SUBSCRIBE_STREAM_MSG,
                                MSG_KEY, "origin originChannel is currently in connecting state",
                                "channel_state", ConnectivityState.CONNECTING.toString(),
                                FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run");
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                ConnectivityState channelState = originChannel.getState(true);
                if(channelState.equals(ConnectivityState.READY)) {
                    ehLogger.log(Level.INFO,
                            SUBSCRIBE_STREAM_MSG,
                            MSG_KEY, "originChannel has been successfully reconnected, resetting attempt",
                            FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run");
                    this.backOffDelay.resetAttemptCount();
                }else{
                   ehLogger.log(Level.WARNING,
                           SUBSCRIBE_STREAM_ERROR,
                           MSG_KEY, "originChannel is not active",
                           "channel_state", channelState.toString(),
                           FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run");
                }
            }catch (IllegalStateException e) {
                // If the ClientHeaderInterceptor cancels and refuses the stream to even connect, it will end up here
                // The ClientHeaderInterceptor cancels the stream if the Auth token cannot be found and makes the error call
                ehLogger.log( Level.WARNING,
                        SUBSCRIBE_STREAM_ERROR,
                        FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run",
                        MSG_KEY, "stream refused to connect",
                        EXCEPTION_KEY,e
                        );
            }
        });
    }

    /**
     * Send acks for a list of messages
     * Acks use the offset, topic and partition
     *
     * @param messages list of messages to be acked
     * @throws EventHubClientException when something goes wrong
     */
    synchronized void sendAcks(List<Message> messages) throws EventHubClientException {
        if(!configuration.getSubscribeConfiguration().isAcksEnabled()){
            throw new EventHubClientException.IncorrectConfiguration(EventHubUtils.formatJson(
                    SUBSCRIBER_ERR,
                    MSG_KEY, "incorrect configuration",
                    FUNCTION_NAME_STRING, "SubscriberClient.sendAcks",
                    CAUSE_KEY, "acks not enabled in config",
                    SUBSCRIBER_CONFIG_STRING, this.configuration.toString() ).toString());
        }

        if (isStreamClosed()) {
            ehLogger.log( Level.FINE,
                    SUBSCRIBE_STREAM_MSG,
                    MSG_KEY, "subscriber closed, building stream",
                    FUNCTION_NAME_STRING, "SubscriberClient.sendAcks");
            createRequiredStream();
        }
        try {
            sendRequiredAcks(messages);
            ehLogger.log( Level.FINE,
                    SUBSCRIBER_MSG,
                    MSG_KEY, "sent acks",
                    "count", messages.size(),
                    FUNCTION_NAME_STRING, "SubscriberClient.sendAcks");
        }catch (IllegalStateException e) {
            ehLogger.log( Level.WARNING,
                    SUBSCRIBE_STREAM_ERROR,
                    MSG_KEY, "Encountered error when sending acks",
                    EXCEPTION_KEY,e
            );
        }
    }

    /**
     * Internal helper to take the list of messages and create a list of ack objects
     *
     * @param messages list of messages to be converted to a list of acks
     * @return list of acks to be sent to eventhub service
     */
    protected List<Ack> buildAckFromMessages(List<Message> messages) {
        List<Ack> acks = new ArrayList<>();
        for (Message message : messages) {
            acks.add(Ack.newBuilder()
                    .setId(message.getId()).setPartition(message.getPartition()).setTopic(message.getTopic())
                    .setOffset(message.getOffset()).setTimestamp(message.getTimestamp()).setZoneId(message.getZoneId())
                    .build());
        }
        return acks;
    }


    /**
     * set the default headers that apply to all subscription types
     */
    private void setDefaultHeaders() {
        Metadata.Key<String> zoneId = Metadata.Key.of("predix-zone-id", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> subscriberName = Metadata.Key.of("subscribername", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> topicHeader = Metadata.Key.of("topic", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> subscribeNewestHeader = Metadata.Key.of("offset-newest", Metadata.ASCII_STRING_MARSHALLER);

        header.put(zoneId, configuration.getZoneID());
        header.put(subscriberName, configuration.getSubscribeConfiguration().getSubscriberName());

        switch (configuration.getSubscribeConfiguration().getSubscribeRecency()) {
            case NEWEST:
                header.put(subscribeNewestHeader, "true");
                break;
            default:
                header.put(subscribeNewestHeader, "false");
        }

        // Multiple topics headers
        if (configuration.getSubscribeConfiguration().getTopics().isEmpty()) {
            ehLogger.log( Level.INFO,
                    SUBSCRIBER_MSG,
                    MSG_KEY,"subscribing to default topic",
                    "topic", configuration.getZoneID() + "_topic",
                    FUNCTION_NAME_STRING, "setDefaultHeaders");
        } else {
            ehLogger.log( Level.INFO,
                   SUBSCRIBER_MSG,
                   MSG_KEY, "subscribing to topic(s)",
                   "topic", configuration.getSubscribeConfiguration().getTopics().toString(),
                   FUNCTION_NAME_STRING, "setDefaultHeaders");
        }

        for (String topic : configuration.getSubscribeConfiguration().getTopics()) {
            header.put(topicHeader, topic);
        }
    }


    /**
     * Attach the subscribe with ack heads to the header, used by Batch Client (if acks enabled) and Ack Client
     */
    void setSubscribeWithAckHeaders() {
        Metadata.Key<String> maxRetries = Metadata.Key.of("max-retries", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> retryInterval = Metadata.Key.of("retry-interval", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> durationBeforeFirstRetry = Metadata.Key.of("duration-before-retry", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> acksEnabled  = Metadata.Key.of("acks", Metadata.ASCII_STRING_MARSHALLER);
        String retryIntervalUnit = "s";
        String durationBeforeFirstRetryUnit = "s";
        header.put(acksEnabled, String.valueOf(true));
        header.put(durationBeforeFirstRetry, String.valueOf(configuration.getSubscribeConfiguration().getDurationBeforeFirstRetry()) + durationBeforeFirstRetryUnit);
        header.put(retryInterval, String.valueOf(configuration.getSubscribeConfiguration().getRetryInterval()) + retryIntervalUnit);
        header.put(maxRetries, String.valueOf(configuration.getSubscribeConfiguration().getMaxRetries()));
    }

    /**
     * Set the subscribe in batch for the stub, usd only by batch but same header
     */
    void setSubscribeInBatchHeaders() {
        Metadata.Key<String> batchSize = Metadata.Key.of("batch-size", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> batchInterval = Metadata.Key.of("batch-interval", Metadata.ASCII_STRING_MARSHALLER);
        String batchIntervalUnit = "ms";
        SubscribeConfiguration subscribeConfiguration = configuration.getSubscribeConfiguration();
        header.put(batchInterval, String.valueOf(subscribeConfiguration.getBatchInterval()) + batchIntervalUnit);
        header.put(batchSize, String.valueOf(subscribeConfiguration.getBatchSize()));
    }

    /**
     * Register a callback and create the required stream. createRequiredStream is done in the children of this class
     * @param callback the callback to pass messages to, must be correct type depending on the subscribe configuration
     * @throws EventHubClientException.SubscribeCallbackException if something goes wrong
     */
    protected synchronized void subscribe(Client.Callback callback) throws EventHubClientException.SubscribeCallbackException {
        validateCallback(callback);
        this.callback = callback;
        if(isStreamClosed()){
            createRequiredStream();
        }
    }

    /**
     * Shutdown the client
     */
    protected synchronized void unsubscribe() {
        this.shutdown("unsubscribing");
    }

    /**
     * NOTE: this method does NOT go through parentStreamObserver's onError method so
     * there is no stream control here (close or reconnect)
     */
    private void throwError(String error) {
        throwError(new EventHubClientException.SubscribeFailureException(error));

    }

    /**
     * NOTE: this method does NOT go through parentStreamObserver's onError method so
     * there is no stream control here (close or reconnect)
     */
    private void throwError(Exception e) {
        ehLogger.log( Level.FINE,
                SUBSCRIBE_STREAM_MSG,
                MSG_KEY,  "passing error to callback",
                EXCEPTION_KEY,e
        );
        if (this.callback != null) {
            callback.onFailure(e);
        }else{
            ehLogger.log( Level.WARNING,
                    SUBSCRIBE_STREAM_MSG,
                    MSG_KEY,  "callback was not defined, error lost",
                    EXCEPTION_KEY, e
                );
        }
    }

    /**
     * Throw an error to the stream
     *
     * @param code        GRPC error code
     * @param description what is the error
     * @param cause       what caused the error
     */
    protected void throwErrorToStream(Status.Code code, String description, Throwable cause) {
        Status status = io.grpc.Status.fromCode(code).withDescription(description).withCause(cause);
        ehLogger.log( Level.WARNING,
                SUBSCRIBE_STREAM_MSG,
                MSG_KEY, "throwing error to parent stream observer",
                "code", code.toString(),
                "description", description,
                EXCEPTION_KEY, cause==null? "null" : cause,
                FUNCTION_NAME_STRING, "SubscribeClient.throwErrorToStream"
        );
        this.parentStreamObserver.onError(new StatusException(status));
    }

    /**
     * tell if the stream is closed or not
     * @return if stream is closed
     */
    protected boolean isStreamClosed() {
        return this.context == null || this.context.isCancelled();
    }

    /**
     * tell if the subscriber is active or not
     *
     * @return if the client is active
     */
    protected synchronized boolean isClientActive() {
        return this.activeClient;
    }

    /**
     * Close the current subscriber stream
     * @param cause why is the stream closing
     */
    private synchronized void closeStream(Throwable cause) {
        if (!this.isStreamClosed()) {
            try {
                this.context.cancel(cause);
                ehLogger.log( Level.INFO,
                        SUBSCRIBE_STREAM_MSG,
                        MSG_KEY, "closing stream",
                        FUNCTION_NAME_STRING, "SubscribeClient.closeStream",
                        CAUSE_KEY, cause == null ? "null": cause
                );
            } catch (IllegalStateException e) {
                ehLogger.log( Level.WARNING,
                        SUBSCRIBE_STREAM_ERROR,
                        MSG_KEY, "error when closing stream",
                        FUNCTION_NAME_STRING, "SubscribeClient.closeStream",
                        CAUSE_KEY, cause == null ? "null": cause,
                        EXCEPTION_KEY, e
                );
            }
        }
    }

    /**
     * Reconnect the stream and then resubscribe the callback
     * The error will be expected as to not propagate to the onFailure callback
     * @param cause what issued the stream reconnect
     */
    protected synchronized void reconnectStream(String cause) {

        ehLogger.log( Level.INFO,    SUBSCRIBE_STREAM_MSG,
                MSG_KEY, "reconnecting stream",
                FUNCTION_NAME_STRING, "reconnectStream",
                CAUSE_KEY, cause);

        expectedError = new Throwable(EventHubUtils.formatJson(
                SUBSCRIBE_STREAM_MSG,
                MSG_KEY, "reconnecting stream",
                FUNCTION_NAME_STRING, "reconnectStream",
                CAUSE_KEY, cause
        ).toString());
        
        closeStream(expectedError);
        createRequiredStream();
    }

    /**
     * Shutdown the steam
     * The error will be expected as to not propagate to the onFailure callback
     * @param cause what issued the stream shutdown
     */
    protected synchronized void shutdown(String cause) {
        String str = EventHubUtils.formatJson(
                SUBSCRIBE_STREAM_MSG,
                MSG_KEY, "closing stream",
                FUNCTION_NAME_STRING, "shutdown",
                CAUSE_KEY, cause
        ).toString();
        ehLogger.log( Level.INFO, str);
        this.activeClient = false;
        this.expectedError = new Throwable(str);
        this.closeStream(this.expectedError);
    }
}
