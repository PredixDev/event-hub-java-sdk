# Event Hub STOMP Sample App

## Prerequsites 
* Read up on the [Event Hub Java SDK](https://github.com/PredixDev/event-hub-java-sdk)
* Create new Event Hub instance and necessary UAA instances ([Predix.io Documentation](https://www.predix.io/docs#qQ46G8jh))
* Give UAA user correct scopes to publish/subscribe
* Download and `mvn clean install` sample app

## Setting up Manifest
Using the `MANIFEST.yml` template, fill in the `env` variables with your information. The instance names are the names used when creating each instance during the previous step.

## Running the sample app
### In CF

Because the sample app needs to bind to both the Event Hub and UAA instances, first push the sample app without starting it

```bash
cf push --no-start
```

Then bind the Event Hub and UAA instance to the sample app

```bash
cf bs <your-app-name> <event-hub-service-instance-name>
cf bs <your-app-name> <uaa-service-instance-name>
```

Now restage and start the app so the bindings take effect

```bash
cf restage <your-app-name>
```

### Locally

NOTE: Not all event hub instances are reachable from outside CF.

The easiest way to run the sample app locally is to change the configuration
from pulling from thr environment variables to explicitly setting them.
```java
 EventHubConfiguration configuration = new EventHubConfiguration.Builder()
                .host("<host>")
                .port(443)
                .zoneID("<eventhub_zone_id>")
                .authURL("<auth_url.com>/oauth/token")
                .clientID("<auth_client_id>")
                .clientSecret("<auth_client_secret>")
                .publishConfiguration(new PublishConfiguration.Builder().build())
                .subscribeConfiguration(new SubscribeConfiguration.Builder().build())
                .build();
```

You can also just set the VCAP_SERVICES, UAA_INSTANCE_NAME, EVENTHUB_INSTANCE_NAME,  CLIENT_ID, and CLIENT_SECRETE
environment variables to match that of a cloud foundry app.

## How to Use
Once the app is bound to the services and started, you should just be able to connect to the
server in a browser and start using event hub.

## Configure Logging
See  sdk-logging

## Tips
If the sample app is not working as expected, here are a couple things to try
* Make sure that the UAA user is authorized via UAAC to talk to the Event Hub instance's zoneID
* If running locally, make sure application is not running within proxy
* Repush the application and add the following two enviroment variables for more detailed logs
```yml
_JAVA_OPTIONS: -Dlogging.level.org.springframework=INFO
```

* When running in debug mode, copy out the token and make sure it has the correct scopes. (Decode at https://jwt.io)
* Reference [Event Hub Java SDK](https://github.com/PredixDev/event-hub-java-sdk) for more details


# Change Log
## 1.1
* Added WebSocket Publish example
* Modified the sample app to use best practices for subscribing 

[![Analytics](https://predix-beacon.appspot.com/UA-82773213-1/event-hub-java-sdk/readme?pixel)](https://github.com/PredixDev)

