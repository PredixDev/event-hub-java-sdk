package com.ge.predix.eventhub.client;

import java.util.List;
import java.util.logging.Level;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.stub.Ack;
import com.ge.predix.eventhub.stub.Messages;
import com.ge.predix.eventhub.stub.PublishRequest;
import com.ge.predix.eventhub.stub.PublishResponse;
import com.ge.predix.eventhub.stub.PublisherGrpc;

import io.grpc.*;
import io.grpc.stub.StreamObserver;

import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.EventHubConstants.*;

import static com.ge.predix.eventhub.EventHubConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.PublishClientConstants.*;
import static com.ge.predix.eventhub.EventHubUtils.checkErrorAllowed;

/**
 * Abstract Publisher Client, handles shared code for Async and Sync Publisher
 */
abstract class PublishClient extends ClientInterface {
    protected static Metadata.Key<String> topic = Metadata.Key.of("topic", Metadata.ASCII_STRING_MARSHALLER);

    protected final EventHubConfiguration configuration;
    protected PublisherGrpc.PublisherStub stub;
    protected Context.CancellableContext context;
    protected StreamObserver<PublishRequest> requestObserver;
    protected StreamObserver<PublishResponse> streamObserver;
    protected  ManagedChannel originChannel;
    protected Messages messages = Messages.getDefaultInstance();
    protected Throwable expectedError = null;

    protected  EventHubLogger ehLogger;
    protected BackOffDelay backOffDelay;
    protected boolean activeClient; // Initially false, true only when createStream() is called, false when shutdown is called (NOT closeStream())

    /**
     * Parent Constructor for the abstract Publisher
     * @param channel: the GRPC Channel used to publish messages
     * @param configuration The configuration of the client
     */
    protected PublishClient(Channel channel, ManagedChannel originChannel, EventHubConfiguration configuration) {
        this.originChannel = originChannel;
        this.ehLogger = new EventHubLogger(this.getClass(), configuration);
        this.backOffDelay = new BackOffDelay(this, ehLogger);
        this.configuration = configuration;
        this.stub = PublisherGrpc.newStub(channel);
        this.activeClient = false;
        if(configuration.getPublishConfiguration().getIgnoredValues().size() != 0){
            ehLogger.log(Level.WARNING, PUBLISHER_MSG,
                    MSG_KEY, "ignoring values from config, don't apply to this publisher type",
                    "ignores", configuration.getPublishConfiguration().getIgnoredValues());
        }
        if (configuration.getReconnectRetryLimit() != null) {
            backOffDelay.setRetryLimit(configuration.getReconnectRetryLimit());
        }
    }


    /**
     * Add message for both ASYC and Sync
     * @param newMessages the new message to be added
     * @return Publish Client
     * @throws EventHubClientException.AddMessageException When there are too many messages queue
     */
    protected synchronized PublishClient addMessages(Messages newMessages) throws EventHubClientException.AddMessageException {
        if (this.messages.getMsgCount() + newMessages.getMsgCount() > this.configuration.getPublishConfiguration().getMaxAddedMessages()) {
            throw new EventHubClientException.AddMessageException(EventHubUtils.formatJson(
                    PUBLISHER_ERROR,
                    MSG_KEY, "to many messages",
                    FUNCTION_NAME_STRING, "PublishClient.addMessages",
                    "currentMessageCount", this.messages.getMsgCount() + newMessages.getMsgCount(),
                    PublisherConfigConstants.MAX_ADDED_MESSAGES_STRING, configuration.getPublishConfiguration().getMaxAddedMessages()
            ).toString());
        }
        messages = messages.toBuilder().addAllMsg(newMessages.getMsgList()).build();
        return this;
    }

    /**
     * Return the configuration currently being used by the publisher
     * @return PublishConfiguration
     */
    public PublishConfiguration getConfiguration() {
        return this.configuration.getPublishConfiguration();
    }

    /**
     * Flush is abstract, up to the children to choose how to construct
     * @return List of acks for messages sent (sync only)
     * @throws EventHubClientException
     */
    abstract List<Ack> flush() throws EventHubClientException;

    /**
     * Set the publish headers of the GRPC channel
     * @param stub the GRPC publisher stub
     * @param configuration the current configuration of the channel
     * @return the updated stub with the headers attached
     */
    protected abstract PublisherGrpc.PublisherStub setPublishModeHeaders(PublisherGrpc.PublisherStub stub, EventHubConfiguration configuration);

    /**
     * Gets called via the onNext() of the StreamObserver<PublishResponse>
     * async will just pass the publishResponse to the callback,
     * sync will use this to create the ack list
     * @param publishResponse the response of a published method
     */
    protected abstract void onNextPublishResponse(PublishResponse publishResponse);

    /**
     * Gets called via onError() of the StreamObserver<PublishResponse>
     * async will just pass the error to the callback
     * sync will add this to a list of errors to be thrown on/after flush()
     * @param s String of the error
     */
    protected void throwError(String s){
        throwError(new EventHubClientException.PublishFailureException(s));
    }

    protected abstract void throwError(Throwable t);

    /**
     * Rests the errors when the stream is recreated
     */
    protected abstract void resetErrors();


    /**
     * Publishes the messages in the queue to event-hub-service
     * if the stream is not active it will create the stream
     * if there are no messages it will not do anything (return 0)
     * if the stream is not open it will call the throwError()
     * otherwise it will try and publish the messages.
     * @return number of messages published.
     * @throws EventHubClientException
     */
    protected int publish() throws EventHubClientException {
        int sent = publish(this.messages);
        this.messages = Messages.getDefaultInstance();
        return sent;
    }

    /**
     * Create teh steram if needed, build the Publish Request with the messages,
     * Attempt to publish the messages on the stub object
     * @param messagesToBeSent the number of messages to be sent
     * @return number of messages published
     * @throws EventHubClientException if something goes wrong during the publish
     */
    protected int publish(Messages messagesToBeSent) throws EventHubClientException {
        // Call createStream() if it has not been called
        if (this.context == null) {
            this.createStream();
        }
        if (messagesToBeSent.getMsgCount() == 0) {
            return 0;
        }

        PublishRequest request = PublishRequest.newBuilder().setMessages(messagesToBeSent).build();
        int sentMessages = messagesToBeSent.getMsgCount();

        if (isStreamClosed()) {
            // reference to grpc-java for status choice: https://github.com/grpc/grpc-java/blob/master/core/src/main/java/io/grpc/Status.java#L150
            throwError(EventHubUtils.formatJson(
                    PUBLISH_STREAM_ERR,
                    MSG_KEY, "stream closed when attempted to publish",
                    FUNCTION_NAME_STRING, "PublishClient.publish").toString());
            return 0;
        }

        try {
            requestObserver.onNext(request);
        } catch (IllegalStateException | NullPointerException e) {
            throwError(e);
            return 0;
        }
        ehLogger.log( Level.FINE,
                PUBLISHER_MSG,
                MSG_KEY, "published messages",
                "numberMessages", sentMessages,
                FUNCTION_NAME_STRING, "PublishClient.publish"
        );
        return sentMessages;
    }


    /**
     * Construct the stream for the publish clients
     * Defines the onNext and onError for the publisher client.
     * OnNext: call the abstract method onNextPubishResponse
     * OnError: check if the error was expected
     *    if it was, clear the expected error
     *    else pass the error to the throwError, close the stream, attempt reconnect
     *
     * Once StreamObserver is defined, start the context, assign the created stream observer
     */
    private synchronized void createStream() {
        this.activeClient = true;
        this.stub = setPublishModeHeaders(this.stub, this.configuration);
        this.streamObserver = new StreamObserver<PublishResponse>() {
            public void onNext(PublishResponse publishResponse) {
                ehLogger.log( Level.FINEST,
                        PUBLISH_STREAM_MSG,
                        FUNCTION_NAME_STRING, "PublishClient.createStream.onNext",
                        MSG_KEY, "received message",
                        "got", publishResponse);
                onNextPublishResponse(publishResponse);
            }

            public void onError(Throwable throwable) {
                if (checkErrorAllowed(throwable, expectedError)) {
                    ehLogger.log( Level.FINEST,
                            PUBLISH_STREAM_ERR,
                            MSG_KEY, "silencing expected error",
                            FUNCTION_NAME_STRING, "PublishClient.createStream.onError",
                            "expected_error", expectedError,
                            EXCEPTION_KEY, throwable
                    );
                    expectedError = null;
                } else {
                    Status status = Status.fromThrowable(throwable).getCode().toStatus(); // Get only the status
                    if (context.isCancelled() || status == Status.CANCELLED) {
                        throwError(EventHubUtils.formatJson(
                                PUBLISH_STREAM_ERR,
                                FUNCTION_NAME_STRING, "PublishClient.createStream.onError",
                                MSG_KEY, "stream already cancelled",
                                EXCEPTION_KEY, throwable
                        ).toString());
                    }
                    // Make sure not already cancelled .. then cancel
                    else {
                        throwError(EventHubUtils.formatJson(
                                PUBLISH_STREAM_ERR,
                                FUNCTION_NAME_STRING, "PublishClient.createStream.onError",
                                MSG_KEY, "stream closing",
                                EXCEPTION_KEY, throwable
                        ).toString());
                        closeStream(throwable);
                    }
                    try {
                        // This will handle reconnect (including deciding whether or not to reconnect)
                        // The conditional above ensures that the context will be closed before this
                        if (isClientActive()) {
                            ehLogger.log( Level.INFO,
                                    PUBLISH_STREAM_MSG,
                                    MSG_KEY, "starting reconnect",
                                    FUNCTION_NAME_STRING, "PublishClient.createStream.onError",
                                    EXCEPTION_KEY, throwable
                            );
                            backOffDelay.initiateReconnect(status);
                        }
                    } catch (EventHubClientException.ReconnectFailedException reconnectFailedException) {
                        throwError(reconnectFailedException);
                        closeStream(reconnectFailedException);
                    }
                }
            }

            public void onCompleted() {
                ehLogger.log(Level.WARNING, "Publish Stream was gracefully closed by the server starting a reconnect", FUNCTION_NAME_STRING, "publish.onComplete");
                try {
                    backOffDelay.initiateReconnect(Status.UNAVAILABLE);
                } catch (EventHubClientException.ReconnectFailedException e) {
                    throwError(e);
                }
            }
        };

        this.context = Context.current().withCancellation();
        this.context.run(() -> {
            try {
                requestObserver = stub.send(streamObserver);
                while(originChannel.getState(true).equals(ConnectivityState.CONNECTING)){
                    try {
                        ehLogger.log(Level.FINEST,
                                PUBLISH_STREAM_MSG,
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
                            PUBLISH_STREAM_MSG,
                            MSG_KEY, "originChannel has been successfully reconnected, resetting attempt",
                            FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run");
                    this.backOffDelay.resetAttemptCount();
                }else{
                    ehLogger.log(Level.WARNING,
                            PUBLISHER_ERROR,
                            MSG_KEY, "originChannel is not active",
                            "channel_state", channelState.toString(),
                            FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run");
                }
            }catch (IllegalStateException e) {
                // If the ClientHeaderInterceptor cancels and refuses the stream to even connect, it will end up here
                // The ClientHeaderInterceptor cancels the stream if the Auth token cannot be found and makes the error call
                ehLogger.log( Level.WARNING,
                        PUBLISHER_ERROR,
                        FUNCTION_NAME_STRING, "SubscriberClient.createRequiredStream.context.run",
                        MSG_KEY, "stream refused to connect",
                        EXCEPTION_KEY,e
                );
            }
        });
    }

    /**
     * Throw an error to the GRPC stream, used in testing to simulate stream errors
     * @param code: the status code that GRPC understands
     * @param description a description of what the error is
     * @param cause the actual throwable that caused the error
     */
    protected void throwErrorToStream(Status.Code code, String description, Throwable cause) {
        Status status = io.grpc.Status.fromCode(code).withDescription(description).withCause(cause);
        ehLogger.log( Level.FINE,
                PUBLISH_STREAM_ERR,
                MSG_KEY, "throwing error to stream observer onError",
                "code", code.toString(),
                "description", description,
                EXCEPTION_KEY, cause==null ? "null": cause,
                FUNCTION_NAME_STRING, "PublishClient.throwErrorToStream"
        );
        this.streamObserver.onError(new StatusException(status));
    }

    /**
     * Is the stream closed? ask me
     * @return if the stream is closed or not
     */
    protected synchronized boolean isStreamClosed() {

        return this.context == null || this.context.isCancelled();
    }

    /**
     * is the publisher client active? ask me
     * @return activeClient
     */
    protected synchronized boolean isClientActive() {
        return this.activeClient;
    }

    /**
     * Close the stream
     * @param cause for closing the stream
     */
    protected synchronized void closeStream(Throwable cause) {
        if (!isStreamClosed()) {
            try {
                this.context.cancel(cause);
                ehLogger.log( Level.INFO,
                        PUBLISH_STREAM_MSG,
                        MSG_KEY, "closing stream",
                        FUNCTION_NAME_STRING, "closeStream",
                        CAUSE_KEY, cause
                );
            } catch (IllegalStateException e) {
                ehLogger.log( Level.WARNING,
                        PUBLISH_STREAM_ERR,
                        MSG_KEY, "error when closing stream",
                        FUNCTION_NAME_STRING, "PublishClient.closeStream",
                        CAUSE_KEY,cause,
                        EXCEPTION_KEY,e
                );
            }
        }
    }

    /**
     * Issue a reconnect.
     * Keep track of the throwable so we don't trigger the onError of the publisher clients for this reconnect
     * @param cause the cause as why we are closing the stream
     * @throws EventHubClientException when something goes wrong
     */
    protected synchronized void reconnectStream(String cause) throws EventHubClientException {
        String str = EventHubUtils.formatJson(
                PUBLISH_STREAM_MSG,
                MSG_KEY, "reconnecting stream",
                FUNCTION_NAME_STRING, "PublishClient.reconnectStream",
                CAUSE_KEY, cause
        ).toString();
        ehLogger.log( Level.INFO,str);
        this.expectedError = new Throwable(str);
        this.closeStream(this.expectedError);
        this.createStream();
        this.resetErrors();
    }


    /**
     * Shutdown the stream
     * @param cause why we are shutting down the stream
     */
    protected synchronized void shutdown(String cause) {

        ehLogger.log( Level.INFO, PUBLISH_STREAM_MSG,
                MSG_KEY, "closing stream",
                FUNCTION_NAME_STRING, "shutdown",
                CAUSE_KEY, cause);

        this.activeClient = false;
        this.expectedError = new Throwable(EventHubUtils.formatJson(
                PUBLISH_STREAM_MSG,
                MSG_KEY, "closing stream",
                FUNCTION_NAME_STRING, "shutdown",
                CAUSE_KEY, cause
        ).toString());
        this.closeStream(expectedError);
    }
}
