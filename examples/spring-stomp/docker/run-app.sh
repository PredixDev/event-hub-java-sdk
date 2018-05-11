#!/usr/bin/env bash
export AUTH_URL=$UAA_AUTH_URL
export CLIENT_ID=$SDK_CLIENT_ID
export CLIENT_SECRET=$SDK_CLIENT_SECRET
export ZONE_ID=$EVENTHUB_ZONEID
export SCOPE_PREFIX=predix-event-hub.zones.
export UAA_INSTANCE_NAME=event-hub-sdk-uaa
export EVENTHUB_INSTANCE_NAME=event-hub-docker

export VCAP_SERVICES=$(printf '{"predix-event-hub": [{"credentials": {"publish": {"protocol_details": [{"protocol": "grpc","uri": "%s:%s"}],"zone-http-header-name": "Predix-Zone-Id","zone-http-header-value": "%s"},"subscribe": {"protocol_details": [{"protocol": "grpc","uri": "%s:%s"}],"zone-http-header-name": "Predix-Zone-Id","zone-http-header-value": "%s"}},"label": "predix-event-hub","name": "event-hub-docker","provider": null,"syslog_drain_url": null,"tags": ["event-hub", "eventhub", "event hub"],"volume_mounts": []}],"predix-uaa": [{"credentials": {"issuerId": "%s","uri": "%s"},"label": "predix-uaa","name": "event-hub-sdk-uaa","plan": "free","provider": null,"syslog_drain_url": null,"tags": [],"volume_mounts": []}]}' "$EVENTHUB_URI" "$EVENTHUB_PORT" "$EVENTHUB_ZONEID" "$EVENTHUB_URI" "$EVENTHUB_PORT" "$EVENTHUB_ZONEID" "$UAA_AUTH_URL" "$UAA_AUTH_URL")


if [ "$ENABLE_TLS" = "false" ]; then
    export TLS_PEM_FILE="none"
elif [[-v "$EVENTHUB_PEM_FILE"]]; then
    export TLS_PEM_FILE=$EVENTHUB_PEM_FILE
fi
java -jar /app/spring-stomp-1.0.0.jar
