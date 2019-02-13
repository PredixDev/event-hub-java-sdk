#!/usr/bin/env bash
# build the vcaps based on the provided values
export AUTH_URL=$UAA_AUTH_URL
export CLIENT_ID=$SDK_CLIENT_ID
export CLIENT_SECRET=$SDK_CLIENT_SECRET
export ZONE_ID=$EVENTHUB_ZONEID
export SCOPE_PREFIX=predix-event-hub.zones.
export BAD_CLIENT_ID=event-hub-sdk-bad
export BAD_CLIENT_SECRET=Chang3m3
export UAA_INSTANCE_NAME=event-hub-sdk-uaa
export EVENTHUB_INSTANCE_NAME=event-hub-docker

export VCAP_SERVICES=$(printf '{"predix-event-hub": [{"credentials": {"publish": {"protocol_details": [{"protocol": "grpc","uri": "%s:%s"}],"zone-http-header-name": "Predix-Zone-Id","zone-http-header-value": "%s"},"subscribe": {"protocol_details": [{"protocol": "grpc","uri": "%s:%s"}],"zone-http-header-name": "Predix-Zone-Id","zone-http-header-value": "%s"}},"label": "predix-event-hub","name": "event-hub-docker","provider": null,"syslog_drain_url": null,"tags": ["event-hub", "eventhub", "event hub"],"volume_mounts": []}],"predix-uaa": [{"credentials": {"issuerId": "%s","uri": "%s"},"label": "predix-uaa","name": "event-hub-sdk-uaa","plan": "free","provider": null,"syslog_drain_url": null,"tags": [],"volume_mounts": []}]}' "$EVENTHUB_URI" "$EVENTHUB_PORT" "$EVENTHUB_ZONEID" "$EVENTHUB_URI" "$EVENTHUB_PORT" "$EVENTHUB_ZONEID" "$UAA_AUTH_URL" "$UAA_AUTH_URL")


if [ "$ENABLE_TLS" = "false" ]; then
    export TLS_PEM_FILE="none"
elif [[-v "$EVENTHUB_PEM_FILE"]]; then
    export TLS_PEM_FILE=$EVENTHUB_PEM_FILE
fi
printenv
if [ "$TEST_COMMAND" = "*" ]; then
    mvn -B clean -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn test -P Sanity
else
    mvn -B clean -Dtest=$TEST_COMMAND -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn test -P Sanity
fi
exit $?
