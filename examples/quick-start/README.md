
## Quick Start
Getting started with Event Hub Java SKD, Take a look at the basic example project

### Step 1:  Define the configuration
The Event Hub Client uses a configuration to define how the client should be operating. There are three configuraiotns
that the client looks at.
1) Event Hub Client Configuration
    * Defines the EventHub Service, Auth information and zone id
2) Publish Configuration
    * Tells the Client to build and how to configure the publisher stream
3) Subscribe Configuration
    * Tells the Client to build and how to configure the subscriber stream

#### Example
 This example will build a configuration that will pull event hub and uaa information from a VCAP_SERVIES env
 the  can publish and subscribe. If needed you can also build by defining the host, port, and auth information yourself.
 See [Event Hub Configuration](#event-hub-configuration) for more information.

```java
EventHubConfiguration config = new EventHubConfiguration.Builder()
    .fromEnvironmentVariables()
    .publishConfiguration(new PublishConfiguration.Builder().build())
    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName("my-unique-subscriber").build())
    .build();

```

### Step 2: Define Callbacks
The Event Hub SDK uses callbacks to pass messages it receives from the service to the user. There can be
some processing in the callback but for maximum throughput the callback should to as quick as possible.
This callback will pass the received messages onto a ConcurrentLinkedQueue so the user can then process them without
blocking the callback's thread.

The callback's interfaces are defined in the Client class.

#### Example
```java
class SubCallback implements Client.SubscribeCallback {
    ConcurrentLinkedQueue<Message> processMsgQueue;
    SubCallback() {
        processMsgQueue = new ConcurrentLinkedQueue();
    }
    @Override
    public void onMessage(Message message) {
        processMsgQueue.add(message);
    }
    @Override
    public void onFailure(Throwable throwable){
        System.out.println(throwable.toString());
    }
}

class PubCallback implements Client.PublishCallback{
    ConcurrentLinkedQueue<Ack> processAckQueue;
    PubCallback(){
        processAckQueue = new ConcurrentLinkedQueue();
    }
    @Override
    public void onAck(List<Ack> list) {
        processAckQueue.addAll(list);
    }

    @Override
    public void onFailure(Throwable throwable) {
        System.out.println(throwable.toString());
    }
}

```
### Step 3: Make the Client and Register Callbacks
Once the config and callbacks are defied, the client can be built and the callbacks can be registered.
#### Example
```java
Client eventHubClient = new Client(config);
myPubCallback = new PubCallback();
mySubCallback = new SubCallback();
eventHubClient.registerPublishCallback(myPubCallback);
eventHubClient.subscribe(mySubCallback);
```

### Step 4: Use the Client, Bring it all together
Bringing steps 1-3 together to write a simple example to publish and subscribe

#### Example
```java
class QuickStart{

    static class SubCallback implements Client.SubscribeCallback {
        ConcurrentLinkedQueue<Message> processMsgQueue;
        SubCallback() {
            processMsgQueue = new ConcurrentLinkedQueue();
        }
        @Override
        public void onMessage(Message message) {
            processMsgQueue.add(message);
        }
        @Override
        public void onFailure(Throwable throwable){
            System.out.println(throwable.toString());
        }
    }

    static class PubCallback implements Client.PublishCallback{
        ConcurrentLinkedQueue<Ack> processAckQueue;
        PubCallback(){
            processAckQueue = new ConcurrentLinkedQueue();
        }
        @Override
        public void onAck(List<Ack> list) {
            processAckQueue.addAll(list);
        }

        @Override
        public void onFailure(Throwable throwable) {
            System.out.println(throwable.toString());
        }
    }

    public static void main(String[] args){
        EventHubConfiguration config = new EventHubConfiguration.Builder()
                    .fromEnvironmentVariables()
                    .publishConfiguration(new PublishConfiguration.Builder().build())
                    .subscribeConfiguration(new SubscribeConfiguration.Builder().subscriberName("my-unique-subscriber").build())
                    .build();

        Client eventHubClient = new Client(config);
        mySubCallback = new SubCallback();
        myPubCallback = new PubCallback();
        int idCount = 0;
        while (true){
            eventHubClient.addMessage("id-"+idCount, "body").flush();
            Thread.sleep(1000);
            while(!mySubCallback.processMsgQueue.isEmpty()){
                System.out.println(EventHubUtils.makeJson(mySubCallback.processMsgQueue.remove()).toString());
            }
            while(!mySubCallback.processAckQueue.isEmpty()){
                System.out.println(EventHubUtils.makeJson(myPubCallback.processAckQueue.remove()).toString());
            }
        }
    }
}

```

[![Analytics](https://ga-beacon.appspot.com/UA-82773213-1/predix-event-hub-sdk/readme?pixel)](https://github.com/PredixDev)