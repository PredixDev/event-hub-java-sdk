package com.ge.predix.eventhub.client;

import com.ge.predix.eventhub.*;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static com.ge.predix.eventhub.EventHubConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.PublishClientConstants.*;

/**
 * Created by 212428471 on 8/14/17.
 * This class is a Async Publisher Client
 * Extends the abstract PublisherClient
 */


class AsyncPublishClient extends PublishClient {
    protected Client.PublishCallback callback;
    private static Metadata.Key<String> syncAcksHeader = Metadata.Key.of("sync-acks", Metadata.ASCII_STRING_MARSHALLER);
    private static Metadata.Key<String> sendAcksInterval = Metadata.Key.of("send-acks-interval", Metadata.ASCII_STRING_MARSHALLER);
    private static Metadata.Key<String> acksHeader = Metadata.Key.of("acks", Metadata.ASCII_STRING_MARSHALLER);
    private static Metadata.Key<String> nacksOnlyHeader = Metadata.Key.of("nacks", Metadata.ASCII_STRING_MARSHALLER);
    private static Metadata.Key<String> cacheAcksHeader = Metadata.Key.of("cache-acks", Metadata.ASCII_STRING_MARSHALLER);


    /**
     * Call the parent constructor
     *
     * @param channel       grpc channel
     * @param configuration configuration of the publisher
     */
    protected AsyncPublishClient(Channel channel, ManagedChannel orginChannel, EventHubConfiguration configuration) {
        super(channel,orginChannel, configuration);
    }

    /**
     * REQUIRED BY PublisherClient
     * add the async headers and whatever is required of the configuration
     *
     * @param stub          the GRPC publisher stub
     * @param configuration the current configuration of the channel
     * @return the stub with attached headers
     */
    protected PublisherGrpc.PublisherStub setPublishModeHeaders(PublisherGrpc.PublisherStub stub, EventHubConfiguration configuration) {
        Metadata header = new Metadata();
        header.put(cacheAcksHeader, "true");
        header.put(syncAcksHeader, "false");
        switch (configuration.getPublishConfiguration().getAckType()) {
            case ACKS_AND_NACKS:
// Default
                header.put(acksHeader, "true");
                header.put(nacksOnlyHeader, "false");
                break;
            case NACKS_ONLY:
                header.put(acksHeader, "true");
                header.put(nacksOnlyHeader, "true");
                break;
            case NONE:
                header.put(acksHeader, "false");
                header.put(nacksOnlyHeader, "false");
                break;
        }

// Do not cache acks/nacks and send immediately
        if (!(configuration.getPublishConfiguration().isCacheAcksAndNacks())) {
            header.removeAll(cacheAcksHeader);
            header.put(cacheAcksHeader, "false");
        }
        if (configuration.getPublishConfiguration().getTopic() == null) {
            ehLogger.log( Level.INFO,
                    PUBLISHER_MSG,
                    MSG_KEY,"publishing to default topic",
                    "topic", configuration.getZoneID() + "_topic",
                    FUNCTION_NAME_STRING, "setPublishModeHeaders");
        } else {
            ehLogger.log( Level.INFO,
                    PUBLISHER_MSG,
                    MSG_KEY, "publishing to topic(s)",
                    "topic", configuration.getSubscribeConfiguration().getTopics().toString(),
                    FUNCTION_NAME_STRING, "setPublishModeHeaders");
            header.put(topic, configuration.getPublishConfiguration().getTopic());
        }

// Add cache interval
        header.put(sendAcksInterval, configuration.getPublishConfiguration().getCacheAckIntervalMillis() + "ms");
        return MetadataUtils.attachHeaders(stub, header);
    }

    /**
     * On each publish response, if there is a callback defined, pass the response to the callback
     *
     * @param publishResponse the response of a published method
     */
    protected synchronized void onNextPublishResponse(PublishResponse publishResponse) {
        ehLogger.log(Level.FINE,
                PUBLISHER_MSG,
                MSG_KEY, "received acks",
                "publishResponse", publishResponse,
                FUNCTION_NAME_STRING, "AsyncPublishClient.onNextPublishResponse");

        if (callback != null) {
            try {
                callback.onAck(publishResponse.getAckList());
            }catch (Exception e){
                ehLogger.log( Level.SEVERE ,
                        PUBLISHER_ERROR,
                        MSG_KEY, "exception caught inside user publisher callback",
                        "publishResponse", EventHubUtils.formatJson(publishResponse),
                        EXCEPTION_KEY,e);
            }
        } else {
            ehLogger.log( Level.WARNING ,
                    PUBLISHER_MSG,
                    MSG_KEY, "no callback defined, publish response lost");
        }

    }

    /**
     * On each publish error, if there is a callback defined, pass the error to the callback
     *
     * @param t Throwable that was the error
     */
    protected synchronized void throwError(Throwable t) {
        if (callback != null) {
            callback.onFailure(t);
        } else {
            ehLogger.log( Level.WARNING,
                     PUBLISHER_ERROR,
                     MSG_KEY, "Received Publish Error with no callback defined");
        }
    }

    /**
     * Async Pub does not do anything on reset Error
     */
    protected void resetErrors() {

    }


    /**
     * Register a callback for the client to pass errors/responses to
     *
     * @param callback the publisher callback object
     * @throws EventHubClientException if something goes wrong
     */
    protected synchronized void registerPublishCallback(Client.PublishCallback callback) throws EventHubClientException {
        this.callback = callback;
    }

    protected synchronized List<Ack> flush() throws EventHubClientException {
        publish();
        return new ArrayList<Ack>();
    }

    /**
     * Return the callback associated with the publisher client
     *
     * @return callback objcet
     */
    protected synchronized Client.PublishCallback getCallback() {
        return callback;
    }


}
