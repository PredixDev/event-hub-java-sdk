# Event Hub Web Socket Example Code

This example shows  how to publish data to Event Hub Service over websocket

This example uses neovisionaries websocket client but others can be used.
Whats import are the headers that ate attached to the socket connection.

This example does not use the Event Hub SDK, nor does the SDK support web socket connections.
This does not require the event hub SDK>
This method should only be used if the GRPC/HTTP2 is not available on the system
as GRPC/HTTP2 has much better performance.

Environment variables:

    AUTH_URL=<auth_url>
    EVENTHUB_URI=<eventhub_host>
    EVENTHUB_PORT=<port>
    CLIENT_SECRET=<client_secret>
    CLIENT_ID=<client_id>
    ZONE_ID=<zone_id>

## Omitted Ack Values
Since the service is designed around GRPC, default values in the ACK are omitted.
Take a look at the Eventhub.proto file to see what these default values are.

## Carbon Copy of Body
Messages published though web sockets are delivered as an exact copy of what is in the
body field of the json. Because of this, it is recommenced to put a json object inside the body instead of a string.
If the body filed contained a string, the surrounding " would be included in the message delivered to the
subscriber. Tagging the message as a web socket publish can also be used to let the client know to un marshal it
correctly.

[![Analytics](https://ga-beacon.appspot.com/UA-82773213-1/predix-event-hub-sdk/readme?pixel)](https://github.com/PredixDev)