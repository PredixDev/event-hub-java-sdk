package com.ge.predix.eventhub.client;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.ge.predix.eventhub.EventHubConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.PublishClientConstants.PUBLISHER_ERROR;
import static com.ge.predix.eventhub.EventHubConstants.PublishClientConstants.PUBLISHER_MSG;
import static com.ge.predix.eventhub.EventHubConstants.PublishClientConstants.PUBLISH_STREAM_ERR;

/**
 * Created by williamgowell on 8/14/17.
 * This is a SyncPublisher implementation of the Publisher Client
 */
class SyncPublisherClient extends PublishClient {

    private static Metadata.Key<String> syncAcksHeader = Metadata.Key.of("sync-acks", Metadata.ASCII_STRING_MARSHALLER);
    private ConcurrentLinkedQueue<Ack> publishResponses;
    private ConcurrentLinkedQueue<Throwable> errors;
    private CountDownLatch finishLatch = new CountDownLatch(0);
    private final CharSequence failedPrecondition = Status.FAILED_PRECONDITION.getCode().toString();

    /**
     * On creation, start the super() publisher client
     *
     * @param channel the GRPC channel to be used
     * @param configuration the configuration for the publisher client
     */
    protected SyncPublisherClient(Channel channel, ManagedChannel originChannel, EventHubConfiguration configuration) {
        super(channel, originChannel, configuration);
        publishResponses = new ConcurrentLinkedQueue<Ack>();
        errors = new ConcurrentLinkedQueue<Throwable>();
    }

    /**
     * Helper method for flush()
     * throw an error if there are any errors in the error queue.
     *
     * @param message the message tied with the error
     * @throws EventHubClientException.SyncPublisherFlushException the exception holding other exceptions
     */
    private void throwStoredErrors(String message, boolean messagesPublished) throws EventHubClientException.SyncPublisherFlushException {

        if (errors.size() != 0) {
            ArrayList<Throwable> errs = new ArrayList<>(errors);
            errors.clear();
            ehLogger.log( Level.WARNING,
                    PUBLISHER_ERROR,
                    MSG_KEY, message,
                    EXCEPTION_KEY, errs.toArray(),
                    "messagesPublished", messagesPublished
                    );
            throw new EventHubClientException.SyncPublisherFlushException(message, errs, messagesPublished);
        }
    }

    /**
     * Required by abstract Publisher
     * Flush the current message queue in a blocking fashion
     *
     * @return a list of PublishResponseAcks for published methods
     * @throws EventHubClientException if something goes wrong, or if there are any errors called to the throwError()
     */
    synchronized List<Ack> flush() throws EventHubClientException {
        throwStoredErrors(EventHubUtils.formatJson(
                PUBLISH_STREAM_ERR,
                MSG_KEY,"error pre publish"
            ).toString(),false);
        int sent = publish();
        throwStoredErrors(EventHubUtils.formatJson(
                PUBLISH_STREAM_ERR,
                MSG_KEY,"error post publish",
                "numberPublished", sent
                ).toString(), true);
        return countDown(sent);
    }


    /**
     * blocks until the amount of acks received equals the number of sent messages
     * @param sent amount of acks to block for
     * @return the list of acks
     * @throws EventHubClientException.SyncPublisherFlushException if any erros are waiting to be thrown.
     */
    private synchronized List<Ack> countDown(int sent) throws EventHubClientException {
        ArrayList<Ack> acks = new ArrayList<>();
        if (sent != 0) {
            finishLatch = new CountDownLatch(sent);
            try {
                finishLatch.await(this.getConfiguration().getTimeout(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                ehLogger.log( Level.WARNING,
                        PUBLISHER_ERROR,
                        MSG_KEY, "Receiving acks while publish thread interrupted",
                        EXCEPTION_KEY, e
                );
            }
            acks.addAll(publishResponses);
            publishResponses.clear();
            if (finishLatch.getCount() > 0) {
                this.throwError(new EventHubClientException.SyncPublisherTimeoutException(sent, acks));
            }
        }

        throwStoredErrors(EventHubUtils.formatJson(
                PUBLISHER_ERROR,
                MSG_KEY, "error post ack countdown",
                "msgPublished",sent,
                "acksReceived", acks.size(),
                FUNCTION_NAME_STRING, "SyncPublishClient.throwStoredErrors").toString()

                , true);

        return acks;
    }

    /**
     * attach the sync publish headers tot he
     *
     * @param stub          the GRPC publisher stub
     * @param configuration the current configuration of the channel
     * @return the GRPC stub with headers attached
     */
    @Override
    protected PublisherGrpc.PublisherStub setPublishModeHeaders(PublisherGrpc.PublisherStub stub, EventHubConfiguration configuration) {
        Metadata header = new Metadata();
        header.put(syncAcksHeader, "true");
        if (configuration.getPublishConfiguration().getTopic() == null) {

        } else {
            header.put(topic, configuration.getPublishConfiguration().getTopic());
        }

        return MetadataUtils.attachHeaders(stub, header);

    }

    /**
     * Required by abstract PublishClient
     * Add the publish response acks to the publish response queue
     *
     * @param publishResponse the response of a published method
     */
    @Override
    protected void onNextPublishResponse(PublishResponse publishResponse) {
        publishResponses.addAll(publishResponse.getAckList());
        for (int i = 0; i < publishResponse.getAckList().size(); i++) {
            finishLatch.countDown();
        }
    }

    /**
     * Required by abstract parent PublisherClient
     * add the error to the error queue so it can be thrown on the next flush()
     *
     * @param t the error to be thrown
     */
    protected synchronized void throwError(Throwable t) {
        ehLogger.log( Level.WARNING, PUBLISHER_ERROR,
                MSG_KEY, "adding message to queue to be thrown later",
                EXCEPTION_KEY, t,
                FUNCTION_NAME_STRING, "SyncPublishClient.throwError");
        if (errors.size() < getConfiguration().getMaxErrorMessages()) {
            errors.add(t);
        } else {
            ehLogger.log( Level.SEVERE,
                    PUBLISHER_ERROR,
                    MSG_KEY,"error not added to error queue, max error limit reached",
                    EXCEPTION_KEY,t,
                    FUNCTION_NAME_STRING, "SyncPublishClient.throwError");
        }
        if (finishLatch.getCount() > 0 || t.toString().contains(failedPrecondition)) {
            finishLatch.countDown();
        }
    }

    /**
     * Reset the errorQueue so on a reconnect we do;nt have the errors from the last connection
     */
    protected void resetErrors() {
        ehLogger.log( Level.INFO,
                PUBLISHER_MSG,
                MSG_KEY, "resetting publisher errors on reconnect",
                FUNCTION_NAME_STRING, "SyncPublisherClient.resetErrors"
        );
        this.errors.clear();
    }

}

