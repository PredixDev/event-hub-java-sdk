package com.ge.predix.eventhub.client;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubUtils;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.SubscriptionRequest;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.logging.Level;

import static com.ge.predix.eventhub.EventHubConstants.FUNCTION_NAME_STRING;
import static com.ge.predix.eventhub.EventHubConstants.MSG_KEY;
import static com.ge.predix.eventhub.EventHubConstants.SubscribeClientConstants.SUBSCRIBER_ERR;

/**
 * Standard Subscribe Client child of Subscribe Client
 * This client recievs messages one by one and can't ack messages
 */
class StandardSubscribeClient extends SubscribeClient{
    protected StreamObserver<Message> streamObserverStandardSubscribe;

    /**
     * Subscribe Client
     *
     * @param channel       the grpc channel to use for subscription streams
     * @param configuration the configuration to be used to configure the subscriber
     */
    protected StandardSubscribeClient(Channel channel, ManagedChannel orginChanel, EventHubConfiguration configuration) {
        super(channel, orginChanel,   configuration);
        stub = MetadataUtils.attachHeaders(stub, header);
    }

    /**
     * Callback of standard subscribe client must be of type Client.SubscribeCallback
     * @param c the callback that was passed to SubscribeClient.subscribe
     * @throws EventHubClientException.SubscribeCallbackException
     */
    @Override
    protected void validateCallback(Client.Callback c) throws EventHubClientException.SubscribeCallbackException {
        if (!(c instanceof Client.SubscribeCallback)) {
            throw new EventHubClientException.SubscribeCallbackException("invalid callback, callback must be type of Client.SubscriberCallback");
        }
    }

    /**
     * This should never get called since a standard subscriber
     * can't send acks
     * @param messages The messages to be acked
     */
    @Override
    protected void sendRequiredAcks(List<Message> messages) {
        ehLogger.log( Level.SEVERE,
                SUBSCRIBER_ERR,
                MSG_KEY, "You can't send acks on a standard subscriber",
                FUNCTION_NAME_STRING, "StandardSubscriber.sendRequiredAcks"
        );
    }


    @Override
    protected void setupRequiredRun() {
        SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setZoneId(configuration.getZoneID())
                .setSubscriber(configuration.getSubscribeConfiguration().getSubscriberName())
                .setInstanceId(configuration.getSubscribeConfiguration().getSubscriberInstance())
                .build();

        streamObserverStandardSubscribe = new StreamObserver<Message>() {
            @Override
            public void onNext(Message message) {
                parentStreamObserver.onNext(message);
            }

            @Override
            public void onError(Throwable throwable) {
                parentStreamObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                parentStreamObserver.onCompleted();
            }
        };
        stub.receive(request, streamObserverStandardSubscribe);
    }

    @Override
    SubscribeConfiguration.SubscribeStreamType getSubscribeType() {
        return SubscribeConfiguration.SubscribeStreamType.STANDARD;
    }
}
