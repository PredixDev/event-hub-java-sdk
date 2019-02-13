/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.predix.eventhub.client;

import static com.ge.predix.eventhub.EventHubConstants.EXCEPTION_NAME_KEY;
import static com.ge.predix.eventhub.EventHubConstants.FUNCTION_NAME_STRING;
import static com.ge.predix.eventhub.EventHubConstants.MSG_KEY;
import static com.ge.predix.eventhub.EventHubConstants.ReconnectConstants.CLIENT_NAME_KEY;
import static com.ge.predix.eventhub.EventHubConstants.ReconnectConstants.RECONNECT_ERR;
import static com.ge.predix.eventhub.EventHubConstants.ReconnectConstants.RECONNECT_MSG;
import static com.ge.predix.eventhub.EventHubConstants.ReconnectConstants.STATUS_KEY;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubLogger;

import io.grpc.Status;

/**
 * This class is responsible for handling the reconnect functionality of the streams, not the channels
 */
class BackOffDelay {
    private final ClientInterface client;
    private final String clientName;
    protected static int[] delayMilliseconds =  {10000, 15000, 30000, 60000, 180000, 300000}; // 10s, 15s, 30s, 1min, 3min, 5min
    protected static long timeForReset = 1800000L; // 30min; If a reconnect has not been called for this long, reset attempt index
    protected final static Double maxJitterFactor = 0.1;
    protected final static int maxRetryLimit = 2 * delayMilliseconds.length;  // Retry limit for most general cases (retries AFTER original call)
    protected final static int limitedRetryLimit = 3;  // Retry Limit for status unknown error
    protected final static int midLengthDelayIndex = delayMilliseconds.length / 2;   // Index of delay array which would be consider mid length delay

    protected final AtomicInteger attempt = new AtomicInteger();
    private final AtomicBoolean attempting = new AtomicBoolean();
    private final AtomicLong lastAttempt = new AtomicLong();
    private volatile boolean loggedReconnectInProgress = false;

    protected volatile AtomicInteger retryLimit;
    private final AtomicLong lastSetRetryCall = new AtomicLong();
    private EventHubLogger ehLogger;
    private final Random rand = new Random();
    private volatile int clientMaxRetries;
 
    BackOffDelay(ClientInterface client, EventHubLogger ehLogger) {
        this.ehLogger = ehLogger;
        this.client = client;
        this.clientName = client != null ? client.getClass().getSimpleName() : "UnknownClient";
        initializeAttempts();
        setClientMaxRetries(maxRetryLimit);
    }

    private void initializeAttempts() {
        this.attempt.set(0);
        this.attempting.set(false);
        this.lastAttempt.set(System.currentTimeMillis());
    }

    private void setClientMaxRetries(int retry) {
        this.clientMaxRetries = retry;
        ehLogger.log(Level.FINE," ClientMaxRetries set to :"+this.clientMaxRetries);
    }

    protected int getClientMaxRetries() {
        return this.clientMaxRetries;
    }

    /**
     * Initiate a reconnect for the client
     *
     * @param status the status code that started teh reconnect
     * @return reconnect count
     * @return reconnect count
     * @throws EventHubClientException.ReconnectFailedException
     */
    public int initiateReconnect(Status status) throws EventHubClientException.ReconnectFailedException {
        switch (status.getCode()) {
            // Exponential backOff reconnect indefinite
            case INTERNAL:  // Cause by Event Hub internal issue
            case UNAVAILABLE: // Caused by Unknown host, OpenSSL issue, Connection reset by peer, Connection timeout,  etc
                this.setRetryLimit(clientMaxRetries,false);
                ehLogger.log(Level.FINE," Attempt Reconnect with clientMaxRetries; "+clientMaxRetries  );
                return this.attemptReconnect(null);

            // Reconnect but with retry limit
            case UNAUTHENTICATED: // Cause by Missing Authorization header (OAuth client is down, bad credentials should have been already caught)
            case UNKNOWN: // Caused by Could not authenticate user, Bad scopes (ZAC Deny)
                this.setRetryLimit(limitedRetryLimit,true);
                ehLogger.log(Level.FINE," Attempt Reconnect with limitedRetryLimit; "+limitedRetryLimit );
                return this.attemptReconnect(midLengthDelayIndex);


            // Do nothing for these cases
            case CANCELLED: // User wanted to cancel
                return 0;

            // If the status is not listed, do not reconnect
            default:
                ehLogger.log( Level.INFO,
                        RECONNECT_MSG,
                        CLIENT_NAME_KEY, clientName,
                        STATUS_KEY, status.getCode().toString(),
                        MSG_KEY, "no reconnect procedure, reconnect not initiated"
                        );
                return 0;
        }
    }

    /**
     * Raises an EventHubAlaram when we are about to exhaust the first pass of the Exponential Reconnect Cycle. 
     * @param attempt  the index in the array. 
     */
    private void raiseAlarm(int attempt) {
        EventHubAlarm alarm = new EventHubAlarm(" Trying Last Attempt " +attempt +" in the Exponential Reconnect Cycle. If there are more retries left, it will keep retrying ! ",EventHubAlarm.TYPE.RECONNECT);
        ehLogger.log( Level.WARNING,
                RECONNECT_MSG,
                CLIENT_NAME_KEY, clientName,
                MSG_KEY, alarm.logAlaram()
                );
    }

    /**
     * Creates thread with a timeout which then calls the client's reconnect function
     *
     * @param index (nullable)   Specify delay index to use (will override current index)
     * @return Delay time in milliseconds
     */
    protected int attemptReconnect(Integer index) throws EventHubClientException.ReconnectFailedException {
        // If there is another reconnect already in place, ignore new requests
        if (this.attempting.get()) {
            if (!loggedReconnectInProgress) {
                ehLogger.log( Level.FINE,
                        RECONNECT_MSG,
                        CLIENT_NAME_KEY, clientName,
                        MSG_KEY, "reconnect already in progress. Ignoring new reconnect request. (This log will show once per reconnect)"
                        );
            }
            loggedReconnectInProgress = true;
            return 0;
        }

        // If there are no more retries left, ignore new requests
        if (isRetryLimitSet() && this.retryLimit.get() <= 0) {
            ehLogger.log( Level.SEVERE,
                    RECONNECT_MSG,
                    CLIENT_NAME_KEY, clientName,
                    MSG_KEY, "No more reconnect retries left, will no longer attempt to connect to Event Hub"
                    );
            throw new EventHubClientException.ReconnectFailedException("No more reconnect retries left for the client " + this.clientName);
        }

        this.attempting.set(true);

        // Reduce retries left if retryLimit is set
        if (isRetryLimitSet()) {
            this.retryLimit.decrementAndGet();
        }

        // Check the time of last attempt to see if attempt index should be reset
        if (System.currentTimeMillis() - this.lastAttempt.get() >= timeForReset) {
            this.attempt.set(0);
            ehLogger.log( Level.FINE,
                    RECONNECT_MSG,
                    CLIENT_NAME_KEY, clientName,
                    MSG_KEY, "reset backOff delay"
                    );
        }

        // Set index if specified
        if (index != null) {
            this.attempt.set(index);
            ehLogger.log( Level.FINE,
                    RECONNECT_MSG,
                    CLIENT_NAME_KEY, clientName,
                    MSG_KEY, "delay override to start around " + getDelay(index) + "ms"
                    );
        }
        else { // Check to raise an alarm if we are about to exhaust one pass of exponential backoff.
            ehLogger.log( Level.WARNING," index:"+ this.attempt.get() % delayMilliseconds.length);
            if((this.attempt.get() % delayMilliseconds.length) == (delayMilliseconds.length - 1 )) {
                raiseAlarm(delayMilliseconds.length);
            }
        }

        final int timeout = addJitter(getDelay(this.attempt.get()), maxJitterFactor);
        Thread delay = new Thread() {
            public void run() {
                attempt.incrementAndGet();
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    ehLogger.log( Level.WARNING,
                            RECONNECT_ERR,
                            MSG_KEY, "reconnect timeout interrupted",
                            "timeout", timeout,
                            "attempt", (attempt.get() - 1)
                            );
                    return;
                }
                lastAttempt.set(System.currentTimeMillis());
                loggedReconnectInProgress = false;
                attempting.set(false);

                // If for some reason (setAuthToken) successfully reconnects, don't reconnect
                if (!client.isStreamClosed()) {
                    ehLogger.log( Level.FINE,
                                RECONNECT_MSG,
                                CLIENT_NAME_KEY, clientName,
                                MSG_KEY, "client already connected, reconnect cancelled",
                                FUNCTION_NAME_STRING, "BackOffDelay.attemptReconnect.run"
                                );
                    return;
                }

                // Ensure that client is not in inactive mode (after shutdown)
                if (!client.isClientActive()) {
                    ehLogger.log( Level.FINE,
                            RECONNECT_MSG,
                            CLIENT_NAME_KEY, clientName,
                            MSG_KEY, "client is no longer active (shutdown or not initialized), reconnect cancelled",
                            CLIENT_NAME_KEY, clientName,
                            FUNCTION_NAME_STRING, "BackOffDelay.attepmtReconnect.run"

                            );
                    return;
                }

                try {
                    client.reconnectStream("From BackOffDelay.attemptReconnect");
                } catch (EventHubClientException e) {
                    ehLogger.log( Level.WARNING ,
                            RECONNECT_ERR,
                            MSG_KEY, "caught exception during reconnect",
                            CLIENT_NAME_KEY, clientName,
                            FUNCTION_NAME_STRING, "BackOffDelay.attemptReconnect.run",
                            EXCEPTION_NAME_KEY, e);

                }
            }
        };

        ehLogger.log( Level.WARNING,
                RECONNECT_MSG,
                CLIENT_NAME_KEY, clientName,
                MSG_KEY, "reconnecting in "+ timeout + " ms",
                FUNCTION_NAME_STRING, "BackOffDelay.attemptReconnect.run"
                );
        delay.start();
        return timeout;
    }

    /**
     * Given the attempt number, will return the correct delay from array
     * Current behavior wraps around indefinitely
     *
     * @param attemptCount The raw attempt number, can exceed array length
     * @return The delay in milliseconds
     */
    protected int getDelay(int attemptCount) {
        if (attemptCount < 0) {
            ehLogger.log(Level.WARNING,
                    RECONNECT_ERR,
                    CLIENT_NAME_KEY, "N/A",
                    MSG_KEY, "Attempt count is negative, using 0 instead "
                    );
            attemptCount = 0;
        }
        int index = attemptCount % delayMilliseconds.length;
        return delayMilliseconds[index];
    }

    /**
     * Sets the retry limit if it has not been set yet
     *
     * @param limit Numbers of retries left (including current retry)
     */
    protected void setRetryLimit(int limit) {
        this.setClientMaxRetries(limit);
    }

    /**
     *
     * @param limit  sets the retry limit for the reconnect call duration as set by the caller
     * @param reconnectCall flag indicating call initiated from a reconnect
     */
    private void setRetryLimit(int limit, boolean reconnectCall) {
        ehLogger.log(Level.INFO, limit + " :" + reconnectCall);
        //  Sets the retryLimit when initialized for first time or when a newer limit is smaller 
        if (this.retryLimit == null || this.retryLimit.get() > limit) {

            this.retryLimit = new AtomicInteger(limit);
            this.lastSetRetryCall.set(System.currentTimeMillis());
            ehLogger.log( Level.INFO,
                        RECONNECT_MSG,
                        CLIENT_NAME_KEY, clientName,
                        "limit", limit,
                        MSG_KEY, "setting retry limit"
                    );

        }else {
            // If the last time setRetryLimit() called was a while ago, this is probably a new error
            if (System.currentTimeMillis() - lastSetRetryCall.get() >= timeForReset) {
                this.removeRetryLimit();
                this.setRetryLimit(limit,reconnectCall);  // Remove old limit to set new one
            }
            this.lastSetRetryCall.set(System.currentTimeMillis());
        }
    }

    /**
     * For removing the retry limit because either a new different error has occurred, or setRetryLimit hasn't been called for a while
     */
    protected void removeRetryLimit() {
        if (isRetryLimitSet()) {
            ehLogger.log( Level.INFO,
                    RECONNECT_MSG,
                    CLIENT_NAME_KEY, clientName,
                    MSG_KEY, "retry limit removed"
                    );
        }
        this.retryLimit = null;
    }

    private boolean isRetryLimitSet() {
        return retryLimit != null;
    }

    /**
     * The prevent the thunder herd problem, add +/-(timeout*factor) variability to the timeout
     *
     * @return new timeout
     */
    protected int addJitter(int timeout, Double factor) {
        // Factor range is between 0 and 1
        factor = factor < 0 ? 0 : factor;
        factor = factor > 1 ? 1 : factor;
        int lowerLimit = (int) (timeout * (1 - factor));
        int differenceFromLowerToUpperLimit = (int) (timeout * factor * 2) > 0 ? (int) (timeout * factor * 2) : 1; // Must be a positive int
        return lowerLimit + rand.nextInt(differenceFromLowerToUpperLimit);
    }

    /**
     * Reset the attempt count on successful connect
     */
    protected void resetAttemptCount() {
        this.attempt.set(0);
    }
}
