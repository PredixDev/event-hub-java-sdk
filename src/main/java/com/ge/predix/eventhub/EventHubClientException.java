/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for dealing with exceptions within the Event Hub SDK
 * Also contains subclasses for more specific exceptions
 */
public class EventHubClientException extends Exception {
    /**
     * General Client Exception and parent  of all possible exceptions
     *
     * @param msg info message for describing the error
     */
    public EventHubClientException(String msg) {
        super(msg);
    }

    /**
     * General Client Exception and parent  of all possible exceptions
     * Wrapper around a throwable object
     *
     * @param throwable what caused an error
     */

    public EventHubClientException(Throwable throwable) {
        super(throwable);
    }

    /**
     * EventHubClientException  called when the the configuration has invalid entries
     */
    public static class InvalidConfigurationException extends EventHubClientException {
        public InvalidConfigurationException(String msg) {
            super(msg);
        }

        public InvalidConfigurationException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * EventHubClientException called when the client fails to reconnect
     */
    public static class ReconnectFailedException extends EventHubClientException {
        public ReconnectFailedException(String msg) {
            super(msg);
        }

        public ReconnectFailedException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * EventHubClientException for when thrown when the user input (id, secret or scopes) is not correct
     */
    public static class AuthenticationException extends EventHubClientException {
        public AuthenticationException(String msg) {
            super(msg);
        }

        public AuthenticationException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * EventHubClientException for when when the OAuth instance does not response as expected (bad json, no response, etc)
     */
    public static class AuthTokenRequestException extends EventHubClientException {
        public AuthTokenRequestException(String msg) {
            super(msg);
        }
    }

    /**
     * EventHubClientException for when publish fails for whatever reason
     */
    public static class PublishFailureException extends EventHubClientException {
        public PublishFailureException(String msg) {
            super(msg);
        }

        public PublishFailureException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * EventHubClientException for when the user adds more message than stated in the event hub configuration
     */
    public static class AddMessageException extends EventHubClientException {
        public AddMessageException(String msg) {
            super(msg);
        }

        public AddMessageException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * EventHubClientException for when subscriber gets an error
     */
    public static class SubscribeFailureException extends EventHubClientException {
        public SubscribeFailureException(String msg) {
            super(msg);
        }

        public SubscribeFailureException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * EventHubClientException for when an incorrect callback was supplied to the client
     */
    public static class SubscribeCallbackException extends EventHubClientException{
        public SubscribeCallbackException(String msg){
            super(msg);
        }
    }

    /**
     * EventHubClientException for when an action is preformed that the configuration did not support
     */
    public static class IncorrectConfiguration extends EventHubClientException{
        public IncorrectConfiguration(String msg) {
            super(msg);
        }
    }

    public static class SyncPublisherTimeoutException extends EventHubClientException{
        int expectedCount;
        List<Ack> receivedAcks;

        public SyncPublisherTimeoutException(int expectedCount, List<Ack> receivedAcks ){
            super(String.format("Publisher Timeout, expected %d, got %d", expectedCount, receivedAcks.size()));
            this.expectedCount = expectedCount;
            this.receivedAcks = receivedAcks;
        }

    }

    /**
     * EventHubClientException for propagating errors to the user during a flush()
     * contains a list of throwables that contains th exceptions that have been thrown in the background
     */
    public static class SyncPublisherFlushException extends EventHubClientException {
        private ArrayList<Throwable> throwables;
        private boolean messagesPublished;
        public SyncPublisherFlushException(String msg, ArrayList<Throwable> throwables, boolean messagesPublished) {
            super(msg);
            this.messagesPublished = messagesPublished;
            this.throwables = throwables;
        }

        public ArrayList<Throwable> getThrowables() {
            return throwables;
        }

        public String toString(){
            StringBuilder str = new StringBuilder(super.toString());
            str.append("With throwables:");
            for(Throwable t: throwables){
                str.append("\n\t").append(t.toString());
            }
            return str.toString();
        }
    }

}
