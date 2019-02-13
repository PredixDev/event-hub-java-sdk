package com.ge.predix.eventhub.client;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.SubscriptionAcks;
import com.ge.predix.eventhub.SubscriptionMessage;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static com.ge.predix.eventhub.EventHubConstants.EXCEPTION_KEY;
import static com.ge.predix.eventhub.EventHubConstants.FUNCTION_NAME_STRING;
import static com.ge.predix.eventhub.EventHubConstants.MSG_KEY;
import static com.ge.predix.eventhub.EventHubConstants.SubscribeClientConstants.SUBSCRIBER_ERR;


/**
 * Created by chandrakasiraju on 10/25/18.
 * This is the client that gets created if you have metrics enabled
 * in the subscribe configuration.
 */
class MetricsSubscribeClient extends SubscribeClient{

    protected StreamObserver<SubscriptionMessage> streamObserverBatching;
    StreamObserver<SubscriptionAcks> streamRequestorForBatch;
    protected  List<Message> subMsgs ;

    /**
     * MetricsSubscribeClient Subscriber
     * The Metrics Subscribe client to handle metrics messages with batching of messages
     * @param channel the grpc channel to use for subscription streams created by the Client object
     * @param configuration the configuration to be used to configure the subscriber
     */
    MetricsSubscribeClient(Channel channel, ManagedChannel orginChannel, EventHubConfiguration configuration) {
        super(channel, orginChannel, configuration);
        setSubscribeInMetricsHeaders();
        setSubscribeInBatchHeaders();
        if(configuration.getSubscribeConfiguration().isAcksEnabled()){
            setSubscribeWithAckHeaders();
        }
        stub = MetadataUtils.attachHeaders(stub, header);
        subMsgs = new ArrayList<Message>();
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

                //clear all previous messages
                subMsgs.clear();

                //send only the real subscription messages to the subscription call back and send metrics
                // messages to metrics callback.
                for (Message msg : subscriptionMessage.getMessages().getMsgList())
                {
                    if (msg.getIsMetricMsg() == true){
                        try {
                            // we assume metrics callback is registered when Meterics subscriber client is created.
                            // and validate callback verifies that. So we kind of assuming it will be there always otherwise
                            // this throw null Exception and we have to change the abstract version of setupRequiredRun in base class.
                            getMetricsCallback().onMessage(msg);
                        }catch (Exception e){
                            ehLogger.log( Level.WARNING,
                                    SUBSCRIBER_ERR,
                                    MSG_KEY, "metric  callback empty",
                                    FUNCTION_NAME_STRING, "MetricsSubscribeClient.setupRequiredRun",
                                    EXCEPTION_KEY,e
                            );
                        }
                    }else{
                        subMsgs.add(msg);
                    }
                }
                parentStreamObserver.onNext(subMsgs);
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
        return SubscribeConfiguration.SubscribeStreamType.METRICS;
    }

    /**
     * Validate the callback so we can make sure the user provided callback is of the right type of callback
     * @param c user defined callback
     * @throws EventHubClientException.SubscribeCallbackException if incorrect callback type
     */
    protected void validateCallback(Client.Callback c) throws EventHubClientException.SubscribeCallbackException {
        if (!(c instanceof Client.SubscribeBatchCallback)) {
            throw new EventHubClientException.SubscribeCallbackException("incorrect callback, must be of type SubscribeBatchCallback if using metrics");
        }

        Client.Callback metricsCallback = this.getMetricsCallback();

        if (!(metricsCallback instanceof Client.MetricsCallBack)) {
            throw new EventHubClientException.SubscribeCallbackException("incorrect callback registered for metrics, must be of type MetricsCallBack if using metrics");
        }
    }


}
