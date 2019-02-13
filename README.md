# Event Hub SDK for Java
Event Hub SDK is a library that helps connect an application to [Event Hub](https://www.predix.io/services/service.html?id=1987)

 - [SDK from Artifactory](#sdk-from-artifactory)
 - [Using Event Hub SDK](#using-event-hub-sdk)
	 - [Event Hub Configuration](#event-hub-configuration)
	 - [Connecting to Event Hub](#connecting-to-event-hub)
	 - [Publishing to Event Hub](#publish)
	 - [Subscribing to Event Hub](#subscribe)
	 - [Closing Connection](#closing-connection)
	 - [Automatic Reconnection](#automatic-reconnection)
	 - [OAuth2 token management](#oauth2-token-management)
 - [Debugging](#debugging)
 - [Building SDK from repository](#building-sdk-from-repository)
 - [Common Issues](#building-sdk-from-repository)
 - [Running Tests](#running-tests)

## SDK from Artifactory
The most simple way to use this SDK is to pull from the Predix Snapshot artifactory repository. Add the following to your pom file:

```xml
<dependencies>
...
  <dependency>
    <groupId>com.ge.predix.eventhub</groupId>
    <artifactId>predix-event-hub-sdk</artifactId>
    <version>2.2.4</version>
  </dependency>
...
</dependencies>
```

### v2 New Features
1) On a SyncPublisher.flush() you can now see any errors that happened during/since the last flush
2) GRPC Library has been upgraded to v1.6.0
3) Multiple topic support
4) Subscribe in batch support
5) Proxy support for HTTP and GRPC 
6) The ability to clone a config object 
7) Support for Subscribe with Batch
8) Only the scopes based on the configuration are requested
9) Logging though SLF4J or JUL in json format



#### v2 Breaking Changes if Upgrading From v1
In version 2 of the SDK there are several breaking changes to keep in mind if you are upgrading from an early version.
1) You must declare a publisher or subscribe configuration when you build the configuration. It will no longer default the 
publisher and subscribe configurations. Failure to do so or if you try and use a method that the client has not been 
configured to use will result in a InvalidConfiguration Exception.
2) flush() on a sync-publisher will now throw a SyncPublisherException if there were any errors
that occurred during/before the flush was called. More details on this exception here TODO 
3) there is now a single .subscribe() method and the client will build the required stream based on the configuration. subscribeWithAck() has been removed
4) The SDK is now compiled in java 1.8
5) There is now a single PublisherConfig and .publisherType tells the type of the publisher

## Using Event Hub SDK
### Event Hub Configuration
A configuration object `EventHubConfiguration` must be provided for the creation of a `Client` to connect to Event Hub.
 The configuration object contains the fields necessary to connect to Event Hub. The following tables describe the 
 configuration fields.

| Required Fields | Type | Description | Default
| --- | --- | --- | ---
| `host` | `String` | Event Hub host | -
| `port` | `int` | Event Hub port | 443
| `zoneID` | `String` | Predix zone ID for Event Hub and OAuth2 client | -
| `clientID`* | `String` | OAuth2 client ID | -
| `clientSecret`* | `String` | OAuth2 client secret | -
| `authURL`* | `String` | URL of OAuth2 provider (e.g. `https://<oauth provider>/oauth/token`) | -
| `publishConfiguration` | `PublishConfiguration` | Defines publish parameters and tell the client to activate the publisher | -
| `subscribeConfiguration` | `SubscribeConfiguration` | Defines subscription parameters and tell the client to activate the subscriber | -
| `loggingConfiguration` | `LoggingConfiguration`|Define how the sdk should log messages | defaultLogger

_* Not required if automaticTokenRenew is false_

| Optional Fields | Type | Description | Default
| --- | --- | --- | ---
| `automaticTokenRenew` | `boolean` | Token is automatically retrieved and cached for connections | `True`
| `authScopes` | `String` | Used to request specific scopes from OAuth issuer | `null`
| `reconnectRetryLimit`| `int` | Specify number of retries to attempt | Depends on connection error

The Event Hub service details can be set through environment variables and/or manually through the builder. 

#### Manual configuration by builder
The most straightforward way to set the required fields using the builder. 
Note that this is not the most effective way though since fields are hardcoded in.

```java
EventHubConfiguration configuration = new EventHubConfiguration.Builder()
          .host("<Event Hub Service Host>")
          .port("<Event Hub Service Port>")
          .zoneID("<Event Hub Zone ID>")
          .clientID("<Client_ID>")
          .clientSecret("<Client_Secret>")
          .authURL("<UAA URI>")
          .build();
```

#### Configuration from environment variables and VCAP_SERVICES
If the application is running in Predix, the most flexible way to connect to Event Hub is to specify the following
details in the environment variables. By doing so, the required configuration fields will be set automatically through 
the method `fromEnvironmentVariables()` (host, port, zone id). Note that `VCAP_SERVICE` will already be populated by 
Cloud Foundry _as long as the Event Hub and UAA instances are bound to your application_.

| Environment Variable Name | Description
| --- | ---
| `EVENTHUB_INSTANCE_NAME` | Event Hub service instance name
| `UAA_INSTANCE_NAME` | UAA service instance name
| `CLIENT_ID` | OAuth2 client ID
| `CLIENT_SECRET` | OAuth2 client secret
| `VCAP_SERVICES` | Defined already by Cloud Foundry if services are bound

```java`
EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder()
    .fromEnvironmentVariables().build();
```

If `EVENTHUB_INSTANCE_NAME` and `UAA_INSTANCE_NAME` are not set through environment variables, the following allows the Event Hub and UAA instance names to be defined in the code rather than in the environment variables.

```java
EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder()
    .fromEnvironmentVariables("<event-hub-instance-service-name>", "<uaa-instance-service-name>")
    .build();
```

The `CLIENT_ID` and `CLIENT_SECRET` can also be set in tandem with `fromEnvironmentVariables(...)`.

```java
// If the clientID and clientSecret are not in environment variables
EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder()
    .clientID("id")
    .clientSecret("secret")
    .fromEnvironmentVariables("event-hub-instance-service-name", "uaa-instance-service-name")
	.build();
```

##### Without Predix UAA instance
The following does not require an UAA instance to be bound. It pulls only the Event Hub information from `VCAP_SERVICES` leaving all OAuth2 details as `null`. This is useful if the OAuth2 or authentication provider is not in Predix. However, `setAuthToken()` must be called to set a token to connect to Event Hub and `automaticTokenRenew()` must be false.
```java
EventHubConfiguration eventHubConfiguration = new EventHubConfiguration.Builder()
    .fromEnvironmentVariables("event-hub-instance-service-name")
    .automaticTokenRenew(false)
    .build();
```

#### Cloneing the Config
If there are multiple clients in the same program that share common config parameters, the config can be converted back into
a builder that can then be modified. This example shows a base builder that contains the event hub service
information and using that builder to create two separate configs.
```java
 EventHubConfiguration baseConfig = new EventHubConfiguration.Builder()
                    .host(System.getenv("EVENTHUB_URI"))
                    .port(Integer.parseInt(System.getenv("EVENTHUB_PORT")))
                    .zoneID(System.getenv("ZONE_ID"))
                    .clientID(System.getenv("CLIENT_ID"))
                    .clientSecret(System.getenv("CLIENT_SECRET"))
                    .authURL(System.getenv("AUTH_URL"))
                    .build();

    EventHubConfiguration publisherOnlyConfiguration = baseConfig.cloneConfig()
            .publishConfiguration(new PublishConfiguration.Builder()
                    .publisherType(PublisherType.ASYNC)
                    .build())
            .build();
    
    
    EventHubConfiguration subscribeOnlyConfig = baseConfig.cloneConfig()
            .subscribeConfiguration(new SubscribeConfiguration.Builder()
                .acksEnabled(acksEnabled)
                .batchingEnabled(true)
                .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
                .subscriberName("my subscriber name")
                .batchSize(100)
                .build())
            .build();
```

The clone functionality is also available at the SubscribeConfiguration and PublishConfiguration level.
```java 
SubscribeConfiguration baseSubscriber = new SubscribeConfiguration.Builder()
            .acksEnabled(acksEnabled)
            .batchingEnabled(true)
            .subscribeRecency(SubscribeConfiguration.SubscribeRecency.NEWEST)
            .batchSize(100)
            .build());
            
EventHubConfiguration subscriber1 = baseConfig.cloneConfig()
    .subscribeConfiguration(baseSubscriber.cloneConfig()
        .subscriberName("subscriber 1")
        .build())
    .build()

EventHubConfiguration subscriber2 = baseConfig.cloneConfig()
    .subscribeConfiguration(baseSubscriber.cloneConfig()
        .subscriberName("subscriber 2")
        .build())
    .build()
        
```


## Publish

There are two publishing modes; `async` and `sync`.  The difference is how the service sends acks back to the client, 
and how the client the handles the flush() functionality and exceptions. 
In general a sync publisher makes handling acks easier but exceptions are bit more complicated. The
async publisher makes handling errors easier, but keeping track of acks/nacks is harder. A sync publisher is also slower since
the service will return the acks as they are generated vs on a async publisher the acks will be batched.

It is recommended that a sync publisher is used only when it is required that certain messages depend on successful previous messages.
In other words, if you only want to publish message 2 iff message 1 was success, then use a sync publisher.



#### Async Publisher Description

For an Async Publisher, a callback must be provided and the client will pass the message
acknowledgments (Ack Object) to the callbacks `onAck(Ack a)` method. Any Exceptions that happen will be passed to the
callback's `onError(Throwable t)` method. It is up to the developer to decide what to do in the case of these errors.

With Async you can define the type of acks that get returned from the service.
There are two types of acknowledgements from Event Hub (not including errors); `acks` and `nacks`

* `acks` are positive acknowledgements for successful message ingestion
* `nacks` are negative acknowledgements in which the `STATUS` field will describe the error

#### Sync Publisher Description 
Sync Publisher should be used when each flush() must be fully completed before the next one should be.
For sync publisher, the flush method will publish all the messages in queue and then blocks until the SDK 
receives all the acks/nacks (or times out after default 10 seconds, can be configured in the SyncPublisherConfiguration). 
A list of Acks will then be returned containing all the acks for the published messages. It is worth noting that under 
the hood a Sync Publisher is a async publisher with a CountDownLatch to block the function.


When the SDK encounters an error on a sync publisher, at any point during execution, it will hold onto it and throw it during the flush() 
in a SyncPublisherFlushException. This exception contains a list of exceptions that , accessible though getExceptions() 
function of the exception object. The SyncPublisherFlushException exception will also contain a message about where in the 
flush operation the exception was thrown. If the message noted it was pre-publish then the SDK encountered an error at 
some point since the last flush(). If the message was post publish then a error was encountered during the actual publish 
operation. In this case it will inform you the number of published messages, but the acks for that flush were lost. For 
most errors, they will either be thrown pre-publish or before 0 messages get published. See [flush](#Client.flush)

### Configuration

You define the type of publisher by setting publishConfiguration option of the `PublishConfiguration.Builder`. If
the builder has values configured that are not for the type then a warning will be logged
listing the values that will be ignored.
#### Shared Values

The field `maxAddedMessages` and `topic` can be used in both `sync` and `async` publish modes:

| Shared Publish Config Options | Type | Description | Default
| --- | --- | --- | ---
| `maxAddedMessages` | `int` | Defines the maximum number of messages that can be added before sending to prevent too many messages from accumulating | `999`
| `topic` | `String` | if provided to the configuration, messages will get published to that subtopic | Default topic
| `publishConfiguration` | `PublishConfiguration.PublisherType` | The type of config to build | PublishConfiguration.PublisherType.ASYNC

```java
EventHubConfiguration configuration = new EventHubConfiguration.Builder()
      ...
      .publishConfiguration(new PublishConfiguration.Builder()
        .publishConfiguration(PublishConfiguration.PublisherType.ASYNC)
        .maxAddedMessages(500)
        .build())
      ...
      .build();
```


Configurations for Sync and Async are done in the same publisher config and are defined in the publisherType of the builder.
If you set a parameter for a mode that is not selected, a warning will be thrown letting you know the parameter is being 
ignored.

#### Async Configuration
 `async` uses `.publisherType(PublishConfiguration.PublisherType.ASYNC)` configuration parameter. 
 Its parameters affect what acknowledgements it receives and at what rate they will be sent from Event Hub.

| Async Configuration Options | Type | Description | Default
| --- | --- | --- | ---
| `ackType` | `AcknowledgementOptions` | Defines what type of acknowledgements are returned | `ACKS_AND_NACKS`
| `cacheAckIntervalMillis` | `long` |  Defines how long Event Hub will cache the acks before sending them. Values range form `100ms` to `1000ms` | `500`

| `AcknowledgementOptions` | Options | Description
| --- | --- | ---
| `ACKS_AND_NACKS` | Receives both `acks` and `nacks`.
| `NACKS_ONLY` | Receives only `nacks`.
| `NONE` | No acknowledgements are received.

```java
EventHubConfiguration configuration = new EventHubConfiguration.Builder()
          ...
          .publishConfiguration(new PublishConfiguraiton.Builder()
            .publisherType(PublishConfiguration.PublisherType.ASYNC)
            .ackType(PublishAsyncConfiguration.AcknowledgementOptions.NONE)
            .cacheAckIntervalMillis(200)
            .build())
          ...
          .build();
```

#### Sync Configuration

`sync` uses the `.publisherType(PublishConfiguration.PublisherType.SYNC)` configuration setting. 
When publishing, the publisher will block and wait for the `acks` and/or `nacks` from Event Hub. 
The total throughput is less than `async` mode due to this wait time.

| Sync Configuration Options | Type | Description | Default
| --- | --- | --- | ---
| `timeout` | `long` | Sets max timeout to wait for `acks` and `nacks` | `10000`

```java
EventHubConfiguration configuration = new EventHubConfiguration.Builder()
          ...
          .publishConfiguration(new PublishConfiguration.Builder()
              .publisherType(PublishConfiguration.PublisherType.SYNC)
	          .timeout(2000)
	          .build())
          ...
          .build();
```

### Sending Messages
There are two steps for sending messages. Queueing and Sending.
Messages are added to the publisher queue with  addMessage() and sent with flush()

#### Adding Messages
Messages are added to the client before sending. A message contents contains the following three fields:

| Message Field | Type | Description
| --- | --- | ---
| `id` | `String` | User set and for the user to identify the `acks` which are returned
| `body`* | `String` | A string to store and send information
| `tag`* | `Map<String,String>` | A map to store and send information
*_optional field_

```java
Message.newBuilder().setId("id").setZoneId("zoneId")
        .setBody(ByteString.copyFromUtf8("body"));
```
        
There are 4 methods to add messages:

```java
eventHubClient.addMessage(String id, String body);
eventHubClient.addMessage(String id, String body, Map<String, String> tags);
eventHubClient.addMessage(Message newMessage);
eventHubClient.addMessages(Messages newMessages);
```

These methods can also be chained together like below.

```java
eventHubClient.addMessages(newMessages).addMessage("1","body").addMessage("2", "body", null);
```

#### Sending Messages

Once messages have been added, flush can be used to send all the messages in the queue.

In `sync` mode, the flush methods return `List<Ack>` which contains the `acks` for the messages sent. 
and can throw a SyncPublisherFlushException.
```java
List<Ack> acks = null;
try{
    acks = syncClient
            .addMessage(id, messageContent, tags)
            .addMessage(message)
            .addMessages(messages)
            .flush();
}catch (EventHubClientException.SyncPublisherFlushException e){
    for(Throwable t : e.getExceptions()){
        t.printStackTrace();
    }
}
```

`async` mode, does not return or throw anything because the acks and errors are received in the registered callback.
```java
asyncClient.addMessage(id, messageContent, tags).flush()
```

### Async Callback
When using `async` mode, a callback is used to receive `acks`, `nacks` and errors. The callback can be any class that 
implements the following interface.

```java
  public interface PublishCallback {
    void onAck(List<Ack> acks);
    void onFailure(Throwable throwable);
  }
```

`acks` and `nacks` are returned through the callback's `onAck()` method.  Connection closure or errors are returned 
through the callback's `onFailure()` . 

Adding your callback is done as follows.
```java
eventHubClient.registerPublishCallback(Client.PublishCallback callback);
```

To reopen the stream after, call either `forceRenewToken()` or `setAuthToken()` which will use the new token and reconnect the stream.

## Subscribe
Subscribe is used to receive messages from Event Hub. 
### Configuration

The fields for a subscribe configuration are as follows.

| Subscribe Configuration Options | Type | Description | Default
| --- | --- | --- | ---
| `subscriberName` | `String` | Sets subscriber name | `default-subscriber-name`
| `subscribeRecency` | `enum` | Obtain all stored messages with `OLDEST` or only message(s) published after the subscription occurs with `NEWEST` | `eventHubConfiguration.SubscribeRecency.OLDEST`
| `enableAcks` | `boolean` | Enabled the client to ack the messages recieved | `false`
| `retryInterval`* | `int` | Set time between each retry attempt when resending messages. | `30 seconds`
| `durationBeforeFirstRetry`* | `String` | Set the time to wait before sending attempting to resend a message.| `30 seconds`
| `enableBatch` | `boolean` | enable batching of subscription messages. | `false`
| `batchSize`* | `int` | Max size of the batches sent to the client at a time, must be between 1 and 10000 | 
| `batchInterval`* | `int` | If the batch size is not reached, how frequently to send the current batch to the client, must be between 100 and 1000 | 

_*Only used with when acks or batching is enabled_

```java
EventHubConfiguration configuration = new EventHubConfiguration.Builder()
          ...
          .subscribeConfiguration(new SubscribeConfiguration.Builder()
          	.subscriberName("name")
          	.enableAcks(true)
          	.retryIntervalSeconds(30)
          	.subscribeRecency(EventHubConfiguration.SubscribeRecency.NEWEST)
          	.build())
          ...
          .build();
          
        Client eventHubClient = new Client(eventHubConfiguration);

```

#### Subscribing

After a `Client` is created, then `subscribe` can be called by providing a callback to receive the messages. 
The Client will then build the required stream based on the subscribeConfiguraiotn supplied to the client during creation.

The subscribe callback can be hot-swapped by recalling `subscribe` with the new callback. 

A received message/message-batch will trigger the `onMessage()` method. 
Similarly to the publish callback, `onFailure()` will be called if there is a stream or connection issue OR if there 
is an exception thrown in the `onMessage()` implementation. 

The callback must implement one of the following interfaces depending if batching was enabled. If the a incorrect 
callback was provided a SubscribeCallbackException will be thrown.

##### Single Message Delivery (Batching disabled)
```java
  public interface SubscribeCallback {
    void onMessage(Message message); //Called when new message received
    void onFailure(Throwable throwable); //Called with stream or connection issue
  }
  
  eventHubClient.subscribe(Client.SubscribeCallback callback);
```
##### Multiple Message Delivery (Batching Enabled)
```java
  public interface SubscribeBatchCallback{
          void onMessage(List<Message> messages);
          void onFailure(Throwable throwable);
      }
  
  eventHubClient.subscribe(Client.SubscribeBatchCallback callback);
```


#### Acks Enabled
If the configuration has acks enabled, all messages sent to the client must be acknowledged back to the service.
This is done using the clients `sendAcks(List<Message> messages)` or `sendAck(Message message)` function. 
Acks must be send back within in `durationBeforeFirstRetry` interval or else the Event Hub Service will retry the 
message. If still no ack was sent the service will retry periodically at a rate defined by `retryInterval` in the configuration.

An ack for a message is identified by its offset and partition in the system, both of which are contained within the received
Message object. 


#### Batching Enabled
If you are expecting a lot of continuous messages, it is recommenced to enabled batching of messages. This will give better 
performance since the messages will be delivered as a single call (batch) instead of individually. We do not guarantee that 
that each batch will contain `batchSize` number of messages.

#### Stop Subscriptions

The Clients `unsubscribe()` function can be called to close the subscription stream. 

```java
eventHubClient.unsubscribe();
```
You can resubscribe to rebuild the stream.

#### Multiple Subscribers

Event Hub will send each unique `subscriberName` or subscription all stored messages. If there are multiple clients 
per `subscriberName` (max of five) those clients will collectively receive all the messages. That is, the stored 
messages will be divided and load balanced among the clients for that `subscriberName`. Note that this can be used to
increase the speed at which messages are retrieved from EventHub for a subscription.

#### Multiple Topics

Event Hub allows users to subscribe to multiple topics. This allows a single subscription to consume all messages published
across default and custom topics associated with their respective zone ID.

```java
//Provide the names of the topic.
String topicSuffix1 = "NewTopic1";
String topicSuffix2 = "NewTopic2";

List<String> topicSuffixes = new ArrayList<String>();
topicSuffixes.add(topicSuffix1);
topicSuffixes.add(topicSuffix2);

EventHubConfiguration configuration_all_topics_subscribe = new EventHubConfiguration.Builder()
        .fromEnvironmentVariables()
        .subscribeConfiguration(new SubscribeConfiguration.Builder()
                .topics(topicSuffixes) //Note the topics(List) vs topic(String)
```

## Logging
The Event hub sdk offers the ability to configure logging though a
configuration object. The skd can either use the default Simple Logging Facade
 4 Java, slf4j, or be routed directly out a console handler of Java Util Logging,
 JUL. It is intended that JUL should only be used for quick start or when having
 a .properties file is not possible.


All logs are printing in json format for easy integration into log searching systems
To prevent the over head of constructing logs, the json is only constructed if
the log level is enabled for the logger.

Internally, all log messages are sent to the EventHubLogger class that decides if the
log messages gets created and then what logger to route messages though.

### LoggerConfiguration
The Eventhub Java SDK supports both Simple Logging Facade 4 Java and Java Util Logging
Ideally the application will use slf4j and configure it though whatever logging framework that slf4j
has been pointed to.
If JUL is enabled then the logger can be configured with the following environment variables, assuming
the logging configuration has been built using .fromEnvironmentVariables
*Caution: finest logging will include sensitive information like OAuth2 client secret and tokens*

| Config | Type | Default | Description |Environment Variable
| --- | --- | --- | --- | ---
| `enableLogs` | `String` | `true` | User set and for the
| `prettyPrintJsonLogs` | `boolean` | `false` | Should the json logs include new line and tab characters
| `useJUL` | `boolean` | `false` | Should the logs use Java Util Logging over SLF4J
| `utilLogLevel` | `LoggerConfig.LogLevel` | Warning | Set the log level for java.util logging
| `utilEnableHTTPLogging` | boolean | `false` | if using JUL, should http logging for token request be enabled


### Configuring SLF4J
Simple Logging Facade for Java allows the end user to specify what logging
backend framework gets used. [This](http://saltnlight5.blogspot.com/2013/08/how-to-configure-slf4j-with-different.html?_sm_au_=isHfJ3VZPDJsVDfs)
blog post goes into detail on how to setup and configure these loggers.
If the sdk is included in a spring application then logs will automatically
routed though Springs SLF4J integration.

#### GRPC logs
If inside a spring app, grpc logging can To enable grpc logging, include
```
io.grpc.netty.level=DEBUG
```
in your .properties file. The logging config does not matter since the grpc libary routes its logs
though its own logger.


## Closing Connection
When the application is done, it is best practice to close all the connections to Event Hub.
```java
eventHubClient.shutdown();
```

### Automatic Reconnection
By default, the SDK will attempt to reconnect to Event Hub upon disconnection. It employs a back off with increasing
 delays which will recycle based on the error. The following are some of the errors and how they are handled.

| Error Code | Possible Reason | Reconnect Behavior
| --- | --- | ---
| `UNAVAILABLE` | Event Hub not available | SDK will try to reconnect until connection is established
| `UNAUTHENTICATED` | Could not retrieve OAuth token | SDK will try to retrieve token repeatedly and if successful, reconnect to Event Hub
| `UNKNOWN` | Permission denied for OAuth user | SDK will try to reconnect for up to 3 times before failing. After failing, no new connection will be made and new `Client` must be created

### Manual Reconnect
If the streams go down for whatever reason, there is a publicly available reconnect method.
```java
    eventhubClient.reconnect()
```
This will shutdown the client, then rebuild the transport layer, then any streams
(publisher or subscriber) that were active.

## OAuth2 token management

#### Renewing Token
Given OAuth2 parameters are set, the default configuration will renew and cache the token as needed. However, there are methods for finer control of the token. `automaticTokenRenew` is an optional configuration which has a default of `True`. Setting to `False` will prevent any automatic retrieval of tokens and the use of the SDK to get a token
```java
EventHubConfiguration configuration = new EventHubConfiguration.Builder()
    ...
    .automaticTokenRenew(Boolean bool)
    .build()
```

If the token needs to be manually updated, this method will do so.
```java
eventHubClient.setAuthToken(String token);
```

Both `forceRenewToken()` and `setAuthToken()` will restart the connection for the client to ensure the new tokens are active.



##### UAA Required Scopes
The SDK will request all the scopes required for the configuration.  
Depending on the number of scopes that are required user has, the token length passed to Event Hub must not exceed 60,000 
characters else the client may not be able to authenticate with Event Hub.
Specifics scopes can also be passed into a `EventHubConfiguration` via `authScopes(String scopes)` 
where `string` is all the scopes comma separated. The following are the scopes that Event Hub uses:

| Scopes | Use
| --- | ---
| predix-event-hub.zones.`<zoneID>`.user | Required for any token
| predix-event-hub.zones.`<zoneID>`.grpc.publish | Required to publish to default topic
| predix-event-hub.zones.`<zoneID>`.grpc.subscribe | Required to subscribe to default topic
| predix-event-hub.zones.`<zoneID>`.`<subTopic>`.grpc.subscribe | Required to subscribe to a subtopic topic
| predix-event-hub.zones.`<zoneID>`.`<subTopic>`.grpc.publish | Required to publish to a subtopic topic

Note: if you are a user of the Power VPC then you need to replace the scope prefix, predix-event-hub, with event-hub-power

If the uaa client-id does not have access to the required scopes a `AuthenticationException` will be thrown.

#### Connecting to Event Hub
After the configuration object is created, a Event Hub `Client` can be made as follows. This client handles both 
publishing and subscribing.

```java
Client eventHubClient  = new Client(EventHubConfiguration configuration);
```
The streams/token will not do anything until the first publish or subscribe request is made.

#### Proxy Support
There is currently proxy support for http (oAuth token request) and grpc. This currently does not work with BlueSSO.
The proxies are set with environment variables. 

| Proxy Environment Variable | Type | Description 
| --- | --- | --- 
| `GRPC_PROXY_EXP` | `String` | The Proxy to establish GRPC connections though, include port
| `PROXY_URI` | `String` |  HTTP Proxy URI for making the oAuth requests
| `PROXY_PORT` | `int` | HTTP proxy port for making the oAuth requests

### Autoclose
Starting in 1.2.9 the SDK implements the AutoCloseable interface. This allowed you to use the client
with a try-with. If any exceptions occur inside this try block the client will automatically call the 
shutdown method.

```java
try (Client c = new Client(eventHubConfigurationSync)) {
    c.subscribe(callback);
    c.addMessage("id", messageBody, null).flush();
    throw new Exception("I encountered an error");
} catch (Exception e) {
    // the shutdown of Client c has automatically been called here
}
```

### Building SDK from repository
To compile the SDK, the Protobuf classes must the generated. The two ways are as follows.

##### Automatically create Protobuf classes from proto file
After pulling the project, do a `mvn clean compile` which will automatically generate the protobuf classes.
The classes will appear in the target/generated-sources
##### Manually create Protobuf classes from proto file
The version of Protobuf used is v3 therefore when compiling the proto
file, be sure to be using version 3 if building Protobuf class manually.
Link to Protobuf compiler protoc (if generating manually): https://github.com/google/protobuf#protocol-compiler-installation

### Common Issues
#### Jetty ALPN/NPN has not been properly configured
In order to use SSL over HTTP2, BoringSSL needs to be used.
The following dependency needs to be added *first* of the dependencies
in the pom.xml of the project using the Java SDK.

```xml

<dependency>
   <groupId>io.netty</groupId>
   <artifactId>netty-tcnative-boringssl-static</artifactId>
   <version>2.0.7.Final</version>
</dependency>

<repositories>
  <repository>
    <id>netty-repo-2</id>
    <url>http://repo1.maven.org/maven2/</url>
  </repository>
</repositories>
```
### Best Practices
See [this](https://github.com/PredixDev/event-hub-java-sample-app) for sample apps, examples, and step-by-step guides on best practices.
### FAQ
1. What is latest stable version of EventHub SDK?
	- v1.2.11 can be used in production now. v2.X.X is also available, but is being reviewed for public release (you can still use it if you're not planning to be in production)
    - Note: for features such as multiple topics, batch subscriptions, and proxy support version v2.x.x is required
2. Does the EventHub SDK have some kind of message re-try mechanism?
	- Subscription with ACK configuration: retryInterval, durationBeforeFirstRetry, maxRetries
    - For publisher, the user application must do the retry if publish response is not successful, to ensure a durable message publish
3. Is there a response NACK or status that is exposed to the end user where you should retry sending a message?
	- Yes, you will usually receive a NACK from the service if there is a payload or user error. In this case, fix the issue and retry publishing
	- Note: must set set AcknowledgementOptions to either NACKS_ONLY OR ACKS_AND_NACKS in publisher configuration
4. Can you explain a bit about the automatic/manual reconnect option currently available in the EventHub client?
	- There are also automatic reconnect attempts when creating the stream
	- There is a user reconnect function to rebuild the channel
5. Is there an API to see if the client is currently connected to EventHub?
	- No, you can use exceptions (see Exceptions section below) and debugging
6. Is the onFailure callback only activated when sending message, or when connection lost regardless?
	- Publisher: it will be called when sending a message
	- Subscriber: can be called at any time in callback
7. Is there some error codes list for EventHub SDK publisher?
	- See Exceptions section below
8. Is there internal caching in EventHub SDK?
	- No, there is not currently any internal caching
9. Is there a way to control the flow of event hub messages that get processed with the SDK?
	- If you use the sync method, you can send 1 message, wait for the ACK, then another message, etc.
	- If you use the async method, you send many messages and process the ACKs in parallel.
	- To slow down the publish flow, the sync publish method is recommended
	- For subscribing, you have control over this since you can control the read pace
10. Is there a way to pool the messages and only process 5 or 10 at a time using the SDK?
	- You can use subscription with ACKs and control when to ACK the messages and use a value higher than the re-delivery default.
	- You will not get the next set of messages until you ack the current set that was sent to you and messages will be redelivered after redelivery interval has passed and you have still not ACKed them.
11. Everything is working on Internet, but not on BLUESSO?
	- Make sure proxy environment variables are set and configured correctly
	- Example:
		- EXPORT GRPC_PROXY_EXP=proxy-src.research.ge.com:8080
		- EXPORT PROXY_URI=proxy-src.research.ge.com
		- EXPORT PROXY_PORT=8080
		- EXPORT HTTP_PROXY_HOST=proxy-src.research.ge.com
		- EXPORT HTTPS_PROXY_HOST=proxy-src.research.ge.com
		- EXPORT HTTP_PROXY_PORT=8080
		- EXPORT HTTPS_PROXY_PORT=8080
12. When publishing, do you need to wait for an ack to determine if the stream is connected?
	- Yes any ACK or NACK will reflect a channel is open because only the service can send an ACK or NACK
13. Is there a better way to determine the connection state other than publishing?
	- There is none at this time. We are currently in the process of opening v2.0 to the community so that these features can be implemented by the community
14. When is the onError called? Is it a result of a user operation such as publish, or could this errors be propagated as a result of a change in the underlying channel?
	- This could be called in both situations
15. When we get failed precondition, does it necessarily mean that the stream is closed or is it a general error? Can we know why it is closed?
	- One of the causes of FAILED PRECONDITION is that the stream is closed when attempting to publish, in which case a connect would need to be done before publishing again
16. Does reconnect check the channel at all? What could be a cause of failure here? Is there a way for us to understand if the error is transient or permanent?
	- Yes, a reconnect failed exception likely implies a non transient failure
	- Causes of failures can include exceptions related to running out of reconnect retries or not having the reconnectRetryLimit set in your configuration file
17. What does the error "got NACK for event id <zone_id>. NACK print: message id: <zone_id>, status: FAILED, description: Too Many Requests." mean?
	- This is due to rate limiting (currently enabled in CF3 Dev)
### Exceptions (v2.0.0)
|            Exception                                  |                                Possible Causes/Issues
| ----------------------------------------------------- |  ------------------------------------------------------------------------------ |
| EventHubClientException                               | 1. Publish to closed stream
|                                                       | 2. Attempting to register or get callback in Non Async mode
|                                                       | 3. Subscribe callback not created
|                                                       | 4. If shutdown when client needs to be active
|                                                       | 5. Issues getting the token or a bad configuration
|                                                       | 6. Missing publish configuration
|                                                       | 7. Client not initialized or is inactive
|                                                       | 8. Subscribe has not been called before sending Acks
| EventHubClientException.AddMessageException           | 1. Message makes queue size more than configured amount
| EventHubClientException.IncorrectConfiguration        | 1. Attempting to register or get callback in Non Async mode
|                                                       | 2. Publisher config not supplied
|                                                       | 3. Can't get callback without a publisher and subscriber config
|                                                       | 4. Subscribe not initialized before sending Acks
|                                                       | 5. Acks not enabled for the subscription or client
| EventHubClientException.SubscribeCallbackException    | 1. Incorrect callback type provided
| EventHubClientException.ReconnectFailedException      | 1. No more reconnect retries left
| EventHubClientException.AuthTokenRequestException     | 1. Can't parse Oauth response into JSON
|                                                       | 2. Response from OAuth2 provider was null
|                                                       | 3. Could not get token from OAuth2 provider
| EventHubClientException.AuthenticationException       | 1. Can't find token in response, incorrect scopes or client info
|                                                       | 2. No token set
| EventHubClientException.SubscribeFailureException     | 1. Callback was not defined
| EventHubClientException.SyncPublisherFlushException   | 1. Any errors in error queue or any errors waiting to be thrown
| EventHubClientException.SyncPublisherTimeoutException | 1. More messages than acks
| EventHubClientException.InvalidConfigurationException | 1. reconnectRetryLimit not set (value must be between 0 and 5)
|                                                       | 2. Not all environment variables are set, instance not found or parsing error
|                                                       | 3. timeout must be positive value
|                                                       | 4. Cache interval can't be smaller than 100ms or greater than 1000ms
|                                                       | 5. Batch size must be between 1 and 10000
|                                                       | 6. retryInterval cannot be smaller than 2 or greater than 10
|                                                       | 7. Could not find gRPC protocol details for Event Hub Service Instance
|                                                       | 8. Could not find gRPC URI in VCAP_SERVICES
|                                                       | 9. Could not find gRPC zoneID in VCAP_SERVICES
|                                                       | 10. Could not parse VCAP_SERVICES or is not set
|                                                       | 11. Error or unable finding instance
| EventHubClientException.PublishFailureException       | 1. Publish failed

## Running Tests
Please refer [Here](./LAUNCHTEST_README.md)

[![Analytics](https://predix-beacon.appspot.com/UA-82773213-1/event-hub-java-sdk/readme?pixel)](https://github.com/PredixDev)
 

