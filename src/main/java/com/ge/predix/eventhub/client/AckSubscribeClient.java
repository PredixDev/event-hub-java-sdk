package com.ge.predix.eventhub.client;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.SubscriptionResponse;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * Created by williamgowell on 9/21/17.
 */
  class AckSubscribeClient extends SubscribeClient {
    protected StreamObserver<Message> streamObserverWithAck;
    private StreamObserver<SubscriptionResponse> streamRequestorForAcks;

    /**
     * Subscribe
     *
     * @param channel       the grpc channel to use for subscription streams
     * @param configuration the configuration to be used to configure the subscriber
     */
    protected AckSubscribeClient(Channel channel, ManagedChannel originChannel, EventHubConfiguration configuration) {
        super(channel, originChannel, configuration);
        setSubscribeWithAckHeaders();
        stub = MetadataUtils.attachHeaders(stub, header);
    }

    @Override
    protected void validateCallback(Client.Callback c) throws EventHubClientException.SubscribeCallbackException {
        if (!(c instanceof Client.SubscribeCallback)) {
            throw new EventHubClientException.SubscribeCallbackException("incorrect callback, must use SubscribeCallback if batching is not enabled");
        }
    }

    @Override
    protected void sendRequiredAcks(List<Message> messages) {
        streamRequestorForAcks.onNext(SubscriptionResponse.newBuilder().addAllAck(buildAckFromMessages(messages)).build());
    }

    @Override
    protected void setupRequiredRun() {
        streamObserverWithAck = new StreamObserver<Message>() {
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
        streamRequestorForAcks = stub.receiveWithAcks(streamObserverWithAck);
    }

    @Override
    SubscribeConfiguration.SubscribeStreamType getSubscribeType() {
        return SubscribeConfiguration.SubscribeStreamType.ACK;
    }
}
