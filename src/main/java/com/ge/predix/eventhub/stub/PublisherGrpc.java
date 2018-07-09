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
 * Publisher service definition
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.14.1)",
    comments = "Source: EventHub.proto")
public class PublisherGrpc {

  private PublisherGrpc() {}

  public static final String SERVICE_NAME = "predix.eventhub.Publisher";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.ge.predix.eventhub.stub.PublishRequest,
      com.ge.predix.eventhub.stub.PublishResponse> METHOD_SEND =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
          generateFullMethodName(
              "predix.eventhub.Publisher", "send"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.PublishRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.ge.predix.eventhub.stub.PublishResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PublisherStub newStub(io.grpc.Channel channel) {
    return new PublisherStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PublisherBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PublisherBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static PublisherFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PublisherFutureStub(channel);
  }

  /**
   * <pre>
   * Publisher service definition
   * </pre>
   */
  public static interface Publisher {

    /**
     * <pre>
     * Send message to topic. Topic name will be in URI
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishRequest> send(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishResponse> responseObserver);
  }

  @io.grpc.ExperimentalApi
  public static abstract class AbstractPublisher implements Publisher, io.grpc.BindableService {

    @java.lang.Override
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishRequest> send(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(METHOD_SEND, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return PublisherGrpc.bindService(this);
    }
  }

  /**
   * <pre>
   * Publisher service definition
   * </pre>
   */
  public static interface PublisherBlockingClient {
  }

  /**
   * <pre>
   * Publisher service definition
   * </pre>
   */
  public static interface PublisherFutureClient {
  }

  public static class PublisherStub extends io.grpc.stub.AbstractStub<PublisherStub>
      implements Publisher {
    private PublisherStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PublisherStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PublisherStub(channel, callOptions);
    }

    @java.lang.Override
    public io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishRequest> send(
        io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishResponse> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(METHOD_SEND, getCallOptions()), responseObserver);
    }
  }

  public static class PublisherBlockingStub extends io.grpc.stub.AbstractStub<PublisherBlockingStub>
      implements PublisherBlockingClient {
    private PublisherBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PublisherBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PublisherBlockingStub(channel, callOptions);
    }
  }

  public static class PublisherFutureStub extends io.grpc.stub.AbstractStub<PublisherFutureStub>
      implements PublisherFutureClient {
    private PublisherFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PublisherFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PublisherFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PublisherFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SEND = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final Publisher serviceImpl;
    private final int methodId;

    public MethodHandlers(Publisher serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.send(
              (io.grpc.stub.StreamObserver<com.ge.predix.eventhub.stub.PublishResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final Publisher serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_SEND,
          asyncBidiStreamingCall(
            new MethodHandlers<
              com.ge.predix.eventhub.stub.PublishRequest,
              com.ge.predix.eventhub.stub.PublishResponse>(
                serviceImpl, METHODID_SEND)))
        .build();
  }
}
