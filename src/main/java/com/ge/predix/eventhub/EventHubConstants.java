package com.ge.predix.eventhub;

/**
 * Created by williamgowell on 10/4/17.
 * This class is for storing commonly used strings
 * in the event hub sdk. Most of theses relate to log statements.
 */
public class EventHubConstants {
    public static final String
        MSG_KEY = "msg",
        EXCEPTION_KEY = "exception",
        EXCEPTION_NAME_KEY="class",
        EXCEPTION_MSG_KEY="msg",
        EXCEPTION_STACK_TRACE_KEY="stack_trace",
        NULL_STRING = "null",
        FUNCTION_NAME_STRING = "function",
        CAUSE_KEY = "cause";

    public static class ClientConfigConstants{
        public static final String
                CONFIG_MSG = "config_msg",
                CONFIG_ERR = "config_err";
    }

    public static class PublisherConfigConstants {
        public static final String
        //toString JSON keys
                CACHE_ACK_INTERVAL_STRING = "cacheAckIntervalMillis",
                TYPE_STRING = "type",
                MAX_ADDED_MESSAGES_STRING = "maxAddedMessages",
                TOPIC_STRING = "topic",
                DEFAULT_TOPIC_NAME = "topic",
                CACHE_ACKS_AND_NACKS_STRING = "cacheAcksAndNacks",
                ACK_OPTIONS_STRING = "acknowledgementOptions",
                TIMEOUT_STRING = "timeout";
    }

    public static class ClientConstants{
        public static final String
            CLIENT_CHANNEL_ERROR = "channel_error",
            CLIENT_CHANNEL_MSG = "channel_msg",
            CLIENT_ERROR = "client_error",
            CLIENT_MSG = "client_msg",
            INVALID_CONFIG_MESSAGE = "action does not match supplied config",
            //String values of stateful variables, used in logging and toString() methods
            HOST_STRING = "host",
            PORT_STRING = "port",

            PUBLISHER_CONFIG_STRING = "publisher_config",
            SUBSCRIBER_CONFIG_STRING = "subscriber_config";
    }

    public static class SubscribeClientConstants {
        public static final String
                SUBSCRIBE_STREAM_ERROR = "subscribe_stream_error",
                SUBSCRIBE_STREAM_MSG = "subscribe_stream_info",
                SUBSCRIBER_MSG = "subscriber_info",
                SUBSCRIBER_ERR = "subscriber_error",
                //invalid config errors'
                SUBSCRIBER_CONFIG_STRING = ClientConstants.SUBSCRIBER_CONFIG_STRING;
    }


    public static class PublishClientConstants{
        public static final String
                PUBLISH_STREAM_MSG = "publisher_stream_info",
                PUBLISH_STREAM_ERR = "publisher_stream_error",
                PUBLISHER_MSG = "publisher_msg",
                PUBLISHER_ERROR = "publisher_error";
    }

    public static class ReconnectConstants{
        public static final String
            RECONNECT_MSG = "reconnect_msg",
            RECONNECT_ERR = "reconnect_err",
            CLIENT_NAME_KEY = "client_name",
            STATUS_KEY = "status";
    }

    public static class HeaderClientInterceptorConstants{
        public static final String
            INTERCEPTOR_MSG = "interceptor_msg",
            INTERCEPTOR_ERR = "interceptor_err";
    }


    public static class EnvironmentVariables {
        public static final String
            SCOPE_PREFIX = "SCOPE_PREFIX",
            PROXY_URI = "PROXY_URI",
            PROXY_PORT = "PROXY_PORT",
            VCAP_SERVICES = "VCAP_SERVICES",
            INSTANCE_NAME = "EVENTHUB_INSTANCE_NAME",
            UAA_INSTANCE_NAME = "UAA_INSTANCE_NAME",
            CLIENT_ID = "CLIENT_ID",
            CLIENT_SECRET = "CLIENT_SECRET",
            ENABLE_LOGS = "EVENT_HUB_ENBALE_ALL_LOGS",
            USE_JUL = "EVENTHUB_ENABLE_SLF4J",
            DEBUG_LEVEL = "EVENTHUB_LOG_LEVEL",
            PRETTY_PRINT_LOG = "EVENTHUB_LOG_PRETTY_PRINT",
            UTIL_HTTP_LOGGING = "EVENTHUB_LOG_HTTP";


    }
}
