/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.predix.eventhub.client;

import com.ge.predix.eventhub.EventHubClientException;
import io.grpc.Status;

/**
 * Interface that the sub-clients use so the backoff delay can call the required methods regardless of the client type
 */
abstract class ClientInterface {
    // This will reconnect the client to Event Hub
    abstract void reconnectStream(String cause) throws EventHubClientException;

    // This will simulate errors coming from the stream (aka Event Hub)
    abstract void throwErrorToStream(Status.Code code, String description, Throwable cause);

    // Stream is closed during reconnect and by errors or shutdown
    abstract boolean isStreamClosed();

    // Client is inactive if publish() has not been called or shutdown() has been called
    abstract boolean isClientActive();
}
