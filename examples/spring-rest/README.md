# Event Hub Sample App

## Prerequsites
* Read up on the [Event Hub Java SDK](https://github.com/PredixDev/predix-event-hub-java-sdk)
* Create new Event Hub instance and necessary UAA instances ([Predix.io Documentation](https://www.predix.io/docs#qQ46G8jh))
* Give UAA user correct scopes to publish/subscribe
* Download and `mvn clean install` sample app

## Running the REST Sample
### Cloud Foundry
#### Setting up Manifest
Using the `MANIFEST.yml` template, fill in the `env` variables with your information.
The instance names are the names used when creating each instance during the previous step.

#### Pushing the sample app
Because the sample app needs to bind to both the Event Hub and UAA instances,
first push the sample app without starting it

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

### Edge Device

The easiest way to setup the sample app on a non-cf machine is to mimic the environment variables
that are in a cloud foundry environment. This involves setting the environment variables
listed in the Manifset.xml and a VCAP_SERVCIES environment variable that matches
that of an app that has been bound to the uaa and event hub service.

An alternative would be modifying the configuration builder to use explicit values
instead the `.fromEnvironmentVariables` function.

### Proxy
See Proxy section of the event hub skd readme.


## How to Use

### Publish SDK

  _Publish a message using the event hub SDK over GRPC_

* **URL**

  /publish

* **Method:**

  `POST`

*  **URL Params**

   **Required:**

   `id=[String]` the message ID

   **Optional:**

   `count=[int]` the number of times the message will get published, will append current number to the message id.

* **Data Body:**

 The body of the publish message

* **Response:**

  The acks received in the form of a JSON Array.

### Get Subscription Messages

  _Get the messages from the eventhub sdk subscriber_

* **URL**

  /subscribe

* **Method:**

  `GET`

* **Response:**

  the messages in JSON from


## Change Log
### 1.0
* Upgraded to Event Hub SDK 2.0
* Added Multiple Topic Support
* Migrated to SDK repo
* Removed Web Socket Publish Example to its own project

### 0.1
* Added WebSocket Publish example
* Modified the sample app to use best practices for subscribing

[![Analytics](https://ga-beacon.appspot.com/UA-82773213-1/predix-event-hub-sdk/readme?pixel)](https://github.com/PredixDev)