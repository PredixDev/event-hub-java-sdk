/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub.configuration;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubUtils;


import java.util.ArrayList;
import java.util.logging.Level;


import static com.ge.predix.eventhub.EventHubConstants.PublisherConfigConstants.*;

public class PublishConfiguration {
    private long timeout;
    private int maxAddedMessages;
    private String topic;
    private int maxErrorMessages;
    private PublisherType type;

    private AcknowledgementOptions acknowledgementOptions;
    private boolean cacheAcksAndNacks;
    private long cacheAckIntervalMillis;
    private ArrayList<Object> ignores;

    public enum PublisherType {
        SYNC("SYNC"), //
        ASYNC("ASYNC"); //

        private final String text;

        private PublisherType(final String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public enum AcknowledgementOptions {
        NACKS_ONLY("NACKS_ONLY"),           // Only Nacks are returned
        ACKS_AND_NACKS("ACKS_AND_NACKS"),   // Both Acks and Nacks are returned
        NONE("NONE");                       // Neither acknowledgements will be returned

        private final String text;

        private AcknowledgementOptions(final String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    /**
     * @return SYNC publish mode
     */
    public PublisherType getPublishMode() {
        return this.type;
    }

    /**
     * @return Enum of the type of acknowledgements expected from Event Hub
     */
    public AcknowledgementOptions getAckType() {
        return acknowledgementOptions;
    }

    /**
     * @return Whether acknowledgements are cached by Event Hub before returning
     */
    public boolean isCacheAcksAndNacks() {
        return cacheAcksAndNacks;
    }

    /**
     * @return The time interval which Event Hub will cache acknowledgements before sending to client
     */
    public long getCacheAckIntervalMillis() {
        return cacheAckIntervalMillis;
    }

    public PublisherType getType(){
        return type;
    }

    public boolean isSyncConfig(){
        return type == PublisherType.SYNC;
    }

    public boolean isAsyncConfig(){
        return type == PublisherType.ASYNC;
    }

    /**
     * @return topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @return The max time the client will wait for acks
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * @return Max number of message which can be added before flush()
     */
    public int getMaxAddedMessages() {
        return maxAddedMessages;
    }

    /**
     * @return The max number of error messages that can be added to the error list
     */
    public int getMaxErrorMessages() {
        return maxErrorMessages;
    }

    public ArrayList getIgnoredValues(){
        return this.ignores;
    }

    public static class Builder {
        //Common
        private int maxAddedMessages = 999;
        private int maxErrorMessages = 100;
        private String topic;
        private PublisherType type;

        //Sync Only
        private Long timeout; // in milliseconds

        //AsyncOnly
        private Long cacheAckIntervalMillis;
        private Boolean cacheAcksAndNacks;
        private AcknowledgementOptions acknowledgementOptions;

        //defaults
        private long defaultChechenIntervalMills =  500;
        private boolean defaultChechenAcksAndNecks = true;
        private long defaultTimeout = 10000;
        private PublisherType defaultType = PublisherType.ASYNC;
        private AcknowledgementOptions deafultAcknowledgementOptions = AcknowledgementOptions.ACKS_AND_NACKS;


        /**
         * Configures the max time the client will wait for acks from Event Hub
         *
         * @param timeout Expressed in milliseconds. Default is 10,000 milliseconds
         * @return Builder
         */
        public Builder timeout(long timeout) throws EventHubClientException.InvalidConfigurationException {
            if(timeout < 0)
                throw new EventHubClientException.InvalidConfigurationException("timeout Must be a Positive Value");
            this.timeout = timeout;
            return this;
        }

        /**
         * Configures the max number of messages that can be added before flush() is called
         *
         * @param maxAddedMessages Max number of added messages. We encourage not exceeding 1000 messages for ideal performance. Default is 999 messages
         * @return Builder
         */
        public Builder maxAddedMessages(int maxAddedMessages) {
            this.maxAddedMessages = maxAddedMessages;
            return this;
        }

        /**
         * Sets topic for published messages. If topic does not exist, it will be created.
         *
         * @param topic
         * @return
         */
        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Set the ack type for this publisher
         * @param acknowledgementOptions
         * @return Builder
         */
        public Builder ackType(AcknowledgementOptions acknowledgementOptions){
            this.acknowledgementOptions = acknowledgementOptions;
            return this;
        }

        /**
         * Configures the max number of errors that can be added to the queue during a flush() call
         * This is to prevent the error queue from growing out of control.
         *
         * @param maxErrorMessages Max number of error messages.
         * @return Builder
         */
        public Builder maxErrorMessages(int maxErrorMessages) {
            this.maxErrorMessages = maxErrorMessages;
            return this;
        }

        public Builder publisherType(PublisherType type){
            this.type = type;
            return this;
        }
        /**
         * Configure whether or not acks are cache before sending to client
         *
         * @param cacheAcksAndNacks Default is true
         * @return Builder
         */
        protected Builder cacheAcksAndNacks(boolean cacheAcksAndNacks) {
            this.cacheAcksAndNacks = cacheAcksAndNacks;
            return this;
        }

        /**
         * The cache interval dictates how long Event Hub will wait and collect acks before sending to the client
         * Overriding this interval is useful in controlling how often acks are returned to the client
         *
         * @param cacheAckIntervalMillis The allowable range is between 100ms to 1 sec with the default being 500ms. The unit is milliseconds
         * @return Builder
         * @throws EventHubClientException.InvalidConfigurationException Thrown if the interval is not within allowable range
         */
        public Builder cacheAckIntervalMillis(long cacheAckIntervalMillis) throws EventHubClientException.InvalidConfigurationException {
            if (cacheAckIntervalMillis < 100 || cacheAckIntervalMillis > 1000) {
                throw new EventHubClientException.InvalidConfigurationException("Cache interval can't be small than 100ms or greater than 1000ms");
            }
            this.cacheAckIntervalMillis = cacheAckIntervalMillis;
            return this;
        }

        public PublishConfiguration build() {
            return new PublishConfiguration(this);
        }
    }

    /**
     * Using the Provided Builder create the Publisher config
     * Warn if a value was set that will not be used for the provided type
     * @param builder the build object to be used to create the config
     */
    private PublishConfiguration(Builder builder) {
        //Common
        this.type = builder.type==null ? builder.defaultType : builder.type;

        this.maxAddedMessages = builder.maxAddedMessages;
        this.topic = builder.topic;
        this.maxErrorMessages = builder.maxErrorMessages;

        ArrayList<Object> ignores = new ArrayList<>();
        if(type.equals(PublisherType.SYNC)){
            this.timeout = builder.timeout==null  ? builder.defaultTimeout : builder.timeout;
            if(builder.cacheAckIntervalMillis != null){
                ignores.add(CACHE_ACK_INTERVAL_STRING);
                ignores.add(builder.cacheAckIntervalMillis);
            }
            if(builder.cacheAcksAndNacks != null){
                ignores.add(CACHE_ACKS_AND_NACKS_STRING);
                ignores.add(builder.cacheAcksAndNacks);
            }
            if(builder.acknowledgementOptions != null){
                ignores.add(ACK_OPTIONS_STRING);
                ignores.add(builder.acknowledgementOptions.toString());
            }
        }else if(type.equals(PublisherType.ASYNC)){
            this.cacheAckIntervalMillis = builder.cacheAckIntervalMillis==null ? builder.defaultChechenIntervalMills : builder.cacheAckIntervalMillis;
            this.cacheAcksAndNacks = builder.cacheAcksAndNacks==null ? builder.defaultChechenAcksAndNecks : builder.cacheAcksAndNacks;
            this.acknowledgementOptions = builder.acknowledgementOptions==null ? builder.deafultAcknowledgementOptions : builder.acknowledgementOptions;
            if(builder.timeout != null){
                ignores.add(TIMEOUT_STRING);
                ignores.add(builder.timeout);
            }
        }
        this.ignores = ignores;
    }

    @Override
    public String toString() {
        if (this.type == PublisherType.SYNC){
            return EventHubUtils.formatJson(
                    TYPE_STRING, this.type.toString(),
                    TOPIC_STRING, this.topic==null ? DEFAULT_TOPIC_NAME : this.topic,
                    MAX_ADDED_MESSAGES_STRING, this.maxAddedMessages
            ).toString();
        }
        else if(this.type == PublisherType.ASYNC){
            return EventHubUtils.formatJson(
                    TYPE_STRING, this.type.toString(),
                    TOPIC_STRING, this.topic==null ? DEFAULT_TOPIC_NAME : this.topic,
                    MAX_ADDED_MESSAGES_STRING, this.maxAddedMessages,
                    CACHE_ACKS_AND_NACKS_STRING, this.cacheAcksAndNacks,
                    ACK_OPTIONS_STRING, this.acknowledgementOptions.toString()).toString();
        }else{
            //Should never get here
            return "{\"null\"}";
        }
    }
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PublishConfiguration that = (PublishConfiguration) o;
        if (this.maxAddedMessages != that.getMaxAddedMessages()) {
            return false;
        }
        if(!this.topic.equals(that.topic)){
            return false;
        }
        if(!this.type.equals(that.type))
        {
            return false;
        }
        if(this.type.equals(PublisherType.ASYNC)){
            if (cacheAckIntervalMillis != that.cacheAckIntervalMillis)
                return false;
            if(!this.acknowledgementOptions.equals(that.acknowledgementOptions))
            {
                return false;
            }
            return cacheAcksAndNacks == that.cacheAcksAndNacks;
        }else if(this.type.equals(PublisherType.SYNC)){
            if(this.timeout != that.timeout){
                return false;
            }
            return this.maxAddedMessages != that.maxAddedMessages;
        }
        return true;
    }
    Builder cloneConfig() throws EventHubClientException.InvalidConfigurationException {
        if(this.getPublishMode().equals(PublisherType.ASYNC)){
            return new Builder()
                    .publisherType(this.type)
                    .ackType(this.acknowledgementOptions)
                    .maxAddedMessages(this.maxAddedMessages)
                    .cacheAckIntervalMillis(this.cacheAckIntervalMillis)
                    .topic(this.topic)
                    .cacheAcksAndNacks(this.cacheAcksAndNacks)
                    .maxErrorMessages(this.maxErrorMessages);
        }
        return new Builder()
                .publisherType(this.type)
                .timeout(this.timeout)
                .topic(this.topic)
                .maxAddedMessages(this.maxAddedMessages)
                .maxErrorMessages(this.maxErrorMessages);
    }

}
