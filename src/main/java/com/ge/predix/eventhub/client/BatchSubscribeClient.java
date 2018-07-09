package com.ge.predix.eventhub.client;

import java.util.List;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import com.ge.predix.eventhub.stub.Message;
import com.ge.predix.eventhub.stub.SubscriptionAcks;
import com.ge.predix.eventhub.stub.SubscriptionMessage;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;


/**
 * Created by williamgowell on 9/21/17.
 * This is the client that gets created if you have batching enabled
 * in the subscribe configuration.
 */
class BatchSubscribeClient extends SubscribeClient{

    protected StreamObserver<SubscriptionMessage> streamObserverBatching;
    StreamObserver<SubscriptionAcks> streamRequestorForBatch;

    /**
     * BatchSubscribeClient Subscriber
     * The Batch Subscribe client to handle batching of messages
     * @param channel the grpc channel to use for subscription streams created by the Client object
     * @param configuration the configuration to be used to configure the subscriber
     */
    BatchSubscribeClient(Channel channel, ManagedChannel orginChannel, EventHubConfiguration configuration) {
        super(channel, orginChannel, configuration);
        setSubscribeInBatchHeaders();
        if(configuration.getSubscribeConfiguration().isAcksEnabled()){
            setSubscribeWithAckHeaders();
        }
        stub = MetadataUtils.attachHeaders(stub, header);
    }

    /**
     * Override as required by Parent Subscriber, Takes in the list of messages and builds
     * a SubscriptionAcks object to be sent down the stream to service
     * @param messages The messages to be acked
     */
    @Override
    protected void sendRequiredAcks(List<Message> messages) {
        streamRequestorForBatch.onNext(SubscriptionAcks.newBuilder().addAllAck(buildAckFromMessages(messages)).build());
    }

    /**
     * Set up the subscribe stream for subscriber in batch
     * Define the stream observer to pass all messages to the parent stream observer
     *  set up the stub.subscribe stream with this streamObserver
     */
    @Override
    protected void setupRequiredRun() {
        streamObserverBatching = new StreamObserver<SubscriptionMessage>() {
            @Override
            public void onNext(SubscriptionMessage subscriptionMessage) {
                parentStreamObserver.onNext(subscriptionMessage.getMessages().getMsgList());
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
        streamRequestorForBatch = stub.subscribe(streamObserverBatching);
    }

    @Override
    SubscribeConfiguration.SubscribeStreamType getSubscribeType() {
        return SubscribeConfiguration.SubscribeStreamType.BATCH;
    }

    /**
     * Validate the callback so we can make sure the user provided callback is of the right type of callback
     * @param c user defined callback
     * @throws EventHubClientException.SubscribeCallbackException if incorrect callback type
     */
    protected void validateCallback(Client.Callback c) throws EventHubClientException.SubscribeCallbackException {
        if (!(c instanceof Client.SubscribeBatchCallback)) {
            throw new EventHubClientException.SubscribeCallbackException("incorrect callback, must be of type SubscribeBatchCallback if using batch");
        }
    }
}
