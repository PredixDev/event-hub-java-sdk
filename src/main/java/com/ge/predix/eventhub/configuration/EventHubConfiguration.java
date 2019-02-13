/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub.configuration;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubUtils;

import java.util.logging.Level;

import static com.ge.predix.eventhub.EventHubConstants.ClientConfigConstants.CONFIG_ERR;
import static com.ge.predix.eventhub.EventHubConstants.ClientConfigConstants.CONFIG_MSG;
import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.*;
import static com.ge.predix.eventhub.EventHubConstants.FUNCTION_NAME_STRING;
import static com.ge.predix.eventhub.EventHubConstants.MSG_KEY;

public class EventHubConfiguration {

    /**
     * Publish modes
     */

    private String host;
    private int port = 443;
    private String zoneID;
    private String clientID;
    private String clientSecret;
    private String authURL;
    private boolean automaticTokenRenew;
    private PublishConfiguration publishConfiguration;
    private SubscribeConfiguration subscribeConfiguration;
    private LoggerConfiguration loggerConfiguration;
    private String authScopes;
    private Integer reconnectRetryLimit;

    public Integer getReconnectRetryLimit() {
        return reconnectRetryLimit;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getZoneID() {
        return zoneID;
    }

    public String getClientID() {
        return clientID;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getAuthURL() {
        return authURL;
    }

    public boolean isAutomaticTokenRenew() {
        return automaticTokenRenew;
    }

    public PublishConfiguration getPublishConfiguration() {
        return publishConfiguration;
    }

    public SubscribeConfiguration getSubscribeConfiguration() {
        return subscribeConfiguration;
    }

    public LoggerConfiguration getLoggerConfiguration(){
        return this.loggerConfiguration;
    }
    public String getAuthScopes() {
        return authScopes;
    }

    public static class Builder {
        // required for Event Hub connection
        private String host;
        private int port = 443;
        private String zoneID;
        private Integer reconnectRetryLimit;
        private LoggerConfiguration loggerConfiguration = null;
        // required unless automaticTokenRenew is false
        private String clientID;
        private String clientSecret;
        private String authURL;

        // optional
        private boolean automaticTokenRenew = true;
        private PublishConfiguration publishConfiguration = null;
        private SubscribeConfiguration subscribeConfiguration = null;

        private String authScopes = null;

        /**
         * Configures the Event Hub host
         *
         * @param host The URI of the Event Hub instance
         * @return Builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Configures the Event Hub port
         *
         * @param port The port of the Event Hub instance
         * @return Builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Configures the Zone ID which the client is publishing to
         *
         * @param zoneID The Zone ID can be found in the VCAP_SERVICES of the Event Hub Instance
         * @return Builder
         */
        public Builder zoneID(String zoneID) {
            this.zoneID = zoneID;
            return this;
        }

        /**
         * Configures the Client ID used to authenticate with the OAuth2 client
         *
         * @param clientID The Client ID for the OAuth2 client
         * @return Builder
         */
        public Builder clientID(String clientID) {
            this.clientID = clientID;
            return this;
        }

        /**
         * Configures the Client secret used to authenticate with the OAuth2 client
         *
         * @param clientSecret The Client secret for the OAuth2 client
         * @return Builder
         */
        public Builder clientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        /**
         * Configures the OAuth2 issuer URI
         *
         * @param authURL The URI to the OAuth2 issuer. For UAA, this can be found in VCAP_SERVICES of the UAA instance under "issuerId"
         * @return Builder
         */
        public Builder authURL(String authURL) {
            this.authURL = authURL;
            return this;
        }

        /**
         * Configures whether or not the SDK will handle getting and renewing token
         *
         * @param automaticTokenRenew Default is true. Requires clientID, clientSecret and authURL to be set
         * @return Builder
         */
        public Builder automaticTokenRenew(boolean automaticTokenRenew) {
            this.automaticTokenRenew = automaticTokenRenew;
            return this;
        }

        /**
         * Configures the publish configuration
         *
         * @param publishConfiguration Publish configuration. Default is the ASYNC configuration
         * @return Builder
         */
        public Builder publishConfiguration(PublishConfiguration publishConfiguration) {
            this.publishConfiguration = publishConfiguration;
            return this;
        }

        /**
         * Set the logger config for the client
         *
         * @param loggerConfiguration the configuration to use
         * @return Builder
         */
        public Builder loggerConfiguraiton(LoggerConfiguration loggerConfiguration){
            this.loggerConfiguration = loggerConfiguration;
            return this;
        }

        /**
         * Configures the subscribe configuration
         *
         * @param subscribeConfiguration Subscribe configuration. Default has the default subscriber name and instance
         * @return Builder
         */
        public Builder subscribeConfiguration(SubscribeConfiguration subscribeConfiguration) {
            this.subscribeConfiguration = subscribeConfiguration;
            return this;
        }

        /**
         * Configures the specific scopes to request from the OAuth issuer
         *
         * @param authScopes Comma delimited scopes. Use only if scope requested must be specific
         * @return Builder
         */
        public Builder authScopes(String authScopes) {
            this.authScopes = authScopes;
            return this;
        }

        /**
         * Configures number of retries for reconnecting to Event Hub Service
         *
         * @param reconnectRetryLimit Integer between 0 and 5 (inclusive).
         * @return Builder
         */
        public Builder reconnectRetryLimit(Integer reconnectRetryLimit) throws EventHubClientException.InvalidConfigurationException {

            if (reconnectRetryLimit >= 1) {
                this.reconnectRetryLimit = reconnectRetryLimit;
                return this;
            } else {
                throw new EventHubClientException.InvalidConfigurationException(EventHubUtils.formatJson(
                        CONFIG_ERR,
                        MSG_KEY,  "reconnectRetryLimit not set. It's value must be a  positive integer.",
                        "timeout provided", reconnectRetryLimit,
                        FUNCTION_NAME_STRING, "EventHubConfiguration.reconnectRetryLimit"
                ).toString());
            }
        }

        /**
         * When both Event Hub and UAA instances are bound to the application and the respective service instance names are defined in the environment variables EVENTHUB_INSTANCE_NAME and UAA_INSTANCE_NAME,
         * this will automatically populate all the required Event Hub configuration fields to connect to Event Hub
         *
         * @return Builder
         * @throws EventHubClientException.InvalidConfigurationException Thrown when the environment variables are not set, instance not found or parsing error
         */
        public Builder fromEnvironmentVariables() throws EventHubClientException.InvalidConfigurationException {
            String eventhubInstanceName = EventHubUtils.getValueFromEnv(INSTANCE_NAME);
            String vcapServices = EventHubUtils.getValueFromEnv(VCAP_SERVICES);
            String uaaInstanceName = EventHubUtils.getValueFromEnv(UAA_INSTANCE_NAME);

            this.clientID = EventHubUtils.getValueFromEnv(CLIENT_ID);
            this.clientSecret = EventHubUtils.getValueFromEnv(CLIENT_SECRET);

            EventHubConfiguration eventHubPartialDetails = EventHubUtils.getEventHubDetailsFromVCAPS(vcapServices, eventhubInstanceName);
            this.host = eventHubPartialDetails.getHost();
            this.port = eventHubPartialDetails.getPort();
            this.zoneID = eventHubPartialDetails.getZoneID();

            this.authURL = EventHubUtils.getUAADetailsFromVCAPS(vcapServices, uaaInstanceName);

            return this;
        }

        /**
         * When the Event Hub instance is bound to the application, the environment variable EVENTHUB_INSTANCE_NAME is defined, and the authURL is provided,
         * this method will populate all the required Event Hub configuration fields to connect to Event Hub.
         *
         * @return Builder
         * @throws EventHubClientException.InvalidConfigurationException Thrown when the environment variables are not set, instance not found or parsing error
         */
        public Builder fromEnvironmentVariables(String authURL) throws EventHubClientException.InvalidConfigurationException {
            String eventhubInstanceName = EventHubUtils.getValueFromEnv(INSTANCE_NAME);
            String vcapServices = EventHubUtils.getValueFromEnv(VCAP_SERVICES);

            this.clientID = EventHubUtils.getValueFromEnv(CLIENT_ID);
            this.clientSecret = EventHubUtils.getValueFromEnv(CLIENT_SECRET);

            EventHubConfiguration eventHubPartialDetails = EventHubUtils.getEventHubDetailsFromVCAPS(vcapServices, eventhubInstanceName);
            this.host = eventHubPartialDetails.getHost();
            this.port = eventHubPartialDetails.getPort();
            this.zoneID = eventHubPartialDetails.getZoneID();

            this.authURL = authURL;


            return this;
        }

        /**
         * When the Event Hub is bound to the application,
         * this will automatically populate the fields relating to Event Hub but NOT fields relating to OAuth2
         * <p>
         * This is useful when the OAuth2 client is not UAA or not in Cloud Foundry. This *must* be used with automaticTokenRenew as false
         *
         * @param eventhubInstanceName Name of Event Hub service instance
         * @return Builder
         * @throws EventHubClientException.InvalidConfigurationException Thrown when instances are not found or parsing error
         */
        public Builder fromEnvironmentVariablesWithInstance(String eventhubInstanceName) throws EventHubClientException.InvalidConfigurationException {
            String vcapServices = EventHubUtils.getValueFromEnv(VCAP_SERVICES);

            EventHubConfiguration eventHubPartialDetails = EventHubUtils.getEventHubDetailsFromVCAPS(vcapServices, eventhubInstanceName);
            this.host = eventHubPartialDetails.getHost();
            this.port = eventHubPartialDetails.getPort();
            this.zoneID = eventHubPartialDetails.getZoneID();

            return this;
        }


        /**
         * When both the Event Hub and UAA instances are bound to the application,
         * this will automatically populate all the required Event Hub configuration fields based on the services names provided in the parameters
         * <p>
         * This is useful when there are multiple Event Hub instances which all share the same or different UAA instances.
         *
         * @param eventhubInstanceName Name of Event Hub service instance
         * @param uaaInstanceName      Name of UAA service instance
         * @return Builder
         * @throws EventHubClientException.InvalidConfigurationException Thrown when instances are not found or parsing error
         */
        public Builder fromEnvironmentVariables(String eventhubInstanceName, String uaaInstanceName) throws EventHubClientException.InvalidConfigurationException {
            String vcapServices = EventHubUtils.getValueFromEnv(VCAP_SERVICES);

// allows these values to be set in code or by environment variables
            if (this.clientID == null) {
                this.clientID = EventHubUtils.getValueFromEnv(CLIENT_ID);
            }
            if (this.clientSecret == null) {
                this.clientSecret = EventHubUtils.getValueFromEnv(CLIENT_SECRET);
            }

            EventHubConfiguration eventHubPartialDetails = EventHubUtils.getEventHubDetailsFromVCAPS(vcapServices, eventhubInstanceName);
            this.host = eventHubPartialDetails.getHost();
            this.port = eventHubPartialDetails.getPort();
            this.zoneID = eventHubPartialDetails.getZoneID();

            this.authURL = EventHubUtils.getUAADetailsFromVCAPS(vcapServices, uaaInstanceName);

            return this;
        }

        /**
         * Builds the complete configuration to create a new Event Hub client
         *
         * @return A new EventHubConfiguration
         * @throws EventHubClientException.InvalidConfigurationException Thrown when not all required fields are set
         */
        public EventHubConfiguration build() throws EventHubClientException.InvalidConfigurationException {
            EventHubUtils.checkNotNull(this.host, "host");
            EventHubUtils.checkNotNull(this.zoneID, "zoneID");

            // if SDK is not retrieving token for client, skip check
            if (this.automaticTokenRenew) {
                EventHubUtils.checkNotNull(this.clientID, "client ID");
                EventHubUtils.checkNotNull(this.clientSecret, "client secret");
                EventHubUtils.checkNotNull(this.authURL, "Auth URL");
            }


            return new EventHubConfiguration(this);
        }
    }

    private EventHubConfiguration(Builder builder) throws EventHubClientException.InvalidConfigurationException {
        if(builder.host == null) {
            throw new EventHubClientException.InvalidConfigurationException("");
        }
        if(builder.zoneID == null){
            throw new EventHubClientException.InvalidConfigurationException("");
        }
        this.host = builder.host;
        this.port = builder.port;
        this.zoneID = builder.zoneID;

        this.automaticTokenRenew = builder.automaticTokenRenew;
        if(builder.automaticTokenRenew){
            if(builder.clientSecret == null){
                throw new EventHubClientException.InvalidConfigurationException("");
            }
            if(builder.clientID == null){
                throw new EventHubClientException.InvalidConfigurationException("");
            }
            if(builder.authURL == null){
                throw new EventHubClientException.InvalidConfigurationException("");
            }
            this.clientID = builder.clientID;
            this.clientSecret = builder.clientSecret;
            this.authURL = builder.authURL;
        }

        this.publishConfiguration = builder.publishConfiguration;
        this.subscribeConfiguration = builder.subscribeConfiguration;
        this.authScopes = builder.authScopes;
        this.reconnectRetryLimit = builder.reconnectRetryLimit;
        if(builder.loggerConfiguration == null){
            this.loggerConfiguration = LoggerConfiguration.defaultLogConfig();
        }else{
            this.loggerConfiguration = builder.loggerConfiguration;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        EventHubConfiguration that = (EventHubConfiguration) o;

        if (port != that.port)
            return false;
        if (host != null ? !host.equals(that.host) : that.host != null)
            return false;
        if (zoneID != null ? !zoneID.equals(that.zoneID) : that.zoneID != null)
            return false;
        if (clientID != null ? !clientID.equals(that.clientID) : that.clientID != null)
            return false;
        if (clientSecret != null ? !clientSecret.equals(that.clientSecret) : that.clientSecret != null)
            return false;
        if (publishConfiguration != null ? !publishConfiguration.equals(that.publishConfiguration) : that.publishConfiguration != null)
            return false;
        if (subscribeConfiguration != null ? !subscribeConfiguration.equals(that.subscribeConfiguration) : that.subscribeConfiguration != null)
            return false;
        if (authScopes != null ? !authScopes.equals(that.authScopes) : that.authScopes != null)
            return false;
        return authURL != null ? authURL.equals(that.authURL) : that.authURL == null;

    }

    public Builder cloneConfig() throws EventHubClientException.InvalidConfigurationException {
       return new Builder()
                .authScopes(this.authScopes)
                .authURL(this.authURL)
                .automaticTokenRenew(this.automaticTokenRenew)
                .clientID(this.clientID)
                .zoneID(this.zoneID)
                .host(this.host)
                .port(this.port)
                .clientSecret(this.clientSecret)
                .subscribeConfiguration(this.subscribeConfiguration==null ? null : this.subscribeConfiguration.cloneConfig().build())
                .publishConfiguration(this.publishConfiguration==null ? null : this.publishConfiguration.cloneConfig().build())
                .loggerConfiguraiton(this.loggerConfiguration==null ? null : this.loggerConfiguration.cloneConfig().build());

    }

    @Override
    public String toString() {
        return EventHubUtils.formatJson(
                "host", host,
                "port",port,
                "zoneID", zoneID == null ? "null" : zoneID,
                "clientID", clientID == null ? "null" : clientID,
                "authURL", authURL == null ? "null" : authURL,
                "providedAuthScopes", authScopes == null ? "null" : authScopes,
                "automaticTokenRenew", automaticTokenRenew,
                "publishConfig", publishConfiguration==null ? "null": publishConfiguration.toString(),
                "subscribeConfig", subscribeConfiguration==null ? "null" : subscribeConfiguration.toString(),
                "loggingConfig", loggerConfiguration).toString();

    }
}
