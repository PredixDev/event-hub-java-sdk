package com.ge.predix.eventhub.stub;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Subscriber service definition
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.14.1)",
    comments = "Source: EventHub.proto")
public class SubscriberGrpc {

  private SubscriberGrpc() {}

  public static final String SERVICE_NAME = "predix.eventhub.Subscriber";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.ge.predix.eventhub.stub.SubscriptionRequest,
      com.ge.predix.eventhub.stub.Message> METHOD_RECEIVE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "predix.eventhub.Subscriber", "receive"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.SubscriptionRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.Message.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.ge.predix.eventhub.stub.SubscriptionResponse,
      com.ge.predix.eventhub.stub.Message> METHOD_RECEIVE_WITH_ACKS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
          generateFullMethodName(
              "predix.eventhub.Subscriber", "receiveWithAcks"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.SubscriptionResponse.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.Message.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.ge.predix.eventhub.stub.SubscriptionAcks,
      com.ge.predix.eventhub.stub.SubscriptionMessage> METHOD_SUBSCRIBE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
          generateFullMethodName(
              "predix.eventhub.Subscriber", "subscribe"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.SubscriptionAcks.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.SubscriptionMessage.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SubscriberStub newStub(io.grpc.Channel channel) {
    return new SubscriberStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SubscriberBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SubscriberBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static SubscriberFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SubscriberFutureStub(channel);
  }

  /**
   * <pre>
   * Subscriber service definition
   * </pre>
   */
  public static interface Subscriber {

    /**
     * <pre>
     * Receive message from a topic, as a subscriber
     * </pre>
     */
    public void receive(com.ge.predix.eventhub.stub.SubscriptionRequest request,
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message> responseObserver);

    /**
     */
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionResponse> receiveWithAcks(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message> responseObserver);

    /**
     */
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionAcks> subscribe(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionMessage> responseObserver);
  }

  @io.grpc.ExperimentalApi
  public static abstract class AbstractSubscriber implements Subscriber, io.grpc.BindableService {

    @java.lang.Override
    public void receive(com.ge.predix.eventhub.stub.SubscriptionRequest request,
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RECEIVE, responseObserver);
    }

    @java.lang.Override
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionResponse> receiveWithAcks(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message> responseObserver) {
      return asyncUnimplementedStreamingCall(METHOD_RECEIVE_WITH_ACKS, responseObserver);
    }

    @java.lang.Override
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionAcks> subscribe(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionMessage> responseObserver) {
      return asyncUnimplementedStreamingCall(METHOD_SUBSCRIBE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return SubscriberGrpc.bindService(this);
    }
  }

  /**
   * <pre>
   * Subscriber service definition
   * </pre>
   */
  public static interface SubscriberBlockingClient {

    /**
     * <pre>
     * Receive message from a topic, as a subscriber
     * </pre>
     */
    public java.util.Iterator<com.ge.predix.eventhub.stub.Message> receive(
        com.ge.predix.eventhub.stub.SubscriptionRequest request);
  }

  /**
   * <pre>
   * Subscriber service definition
   * </pre>
   */
  public static interface SubscriberFutureClient {
  }

  public static class SubscriberStub extends io.grpc.stub.AbstractStub<SubscriberStub>
      implements Subscriber {
    private SubscriberStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SubscriberStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SubscriberStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SubscriberStub(channel, callOptions);
    }

    @java.lang.Override
    public void receive(com.ge.predix.eventhub.stub.SubscriptionRequest request,
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_RECEIVE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionResponse> receiveWithAcks(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(METHOD_RECEIVE_WITH_ACKS, getCallOptions()), responseObserver);
    }

    @java.lang.Override
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionAcks> subscribe(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionMessage> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(METHOD_SUBSCRIBE, getCallOptions()), responseObserver);
    }
  }

  public static class SubscriberBlockingStub extends io.grpc.stub.AbstractStub<SubscriberBlockingStub>
      implements SubscriberBlockingClient {
    private SubscriberBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SubscriberBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SubscriberBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SubscriberBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public java.util.Iterator<com.ge.predix.eventhub.stub.Message> receive(
        com.ge.predix.eventhub.stub.SubscriptionRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_RECEIVE, getCallOptions(), request);
    }
  }

  public static class SubscriberFutureStub extends io.grpc.stub.AbstractStub<SubscriberFutureStub>
      implements SubscriberFutureClient {
    private SubscriberFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SubscriberFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SubscriberFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SubscriberFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_RECEIVE = 0;
  private static final int METHODID_RECEIVE_WITH_ACKS = 1;
  private static final int METHODID_SUBSCRIBE = 2;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final Subscriber serviceImpl;
    private final int methodId;

    public MethodHandlers(Subscriber serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RECEIVE:
          serviceImpl.receive((com.ge.predix.eventhub.stub.SubscriptionRequest) request,
              (io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RECEIVE_WITH_ACKS:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.receiveWithAcks(
              (io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.Message>) responseObserver);
        case METHODID_SUBSCRIBE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.subscribe(
              (io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.SubscriptionMessage>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final Subscriber serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_RECEIVE,
          asyncServerStreamingCall(
            new MethodHandlers<
              com.ge.predix.eventhub.stub.SubscriptionRequest,
              com.ge.predix.eventhub.stub.Message>(
                serviceImpl, METHODID_RECEIVE)))
        .addMethod(
          METHOD_RECEIVE_WITH_ACKS,
          asyncBidiStreamingCall(
            new MethodHandlers<
              com.ge.predix.eventhub.stub.SubscriptionResponse,
              com.ge.predix.eventhub.stub.Message>(
                serviceImpl, METHODID_RECEIVE_WITH_ACKS)))
        .addMethod(
          METHOD_SUBSCRIBE,
          asyncBidiStreamingCall(
            new MethodHandlers<
              com.ge.predix.eventhub.stub.SubscriptionAcks,
              com.ge.predix.eventhub.stub.SubscriptionMessage>(
                serviceImpl, METHODID_SUBSCRIBE)))
        .build();
  }
}
