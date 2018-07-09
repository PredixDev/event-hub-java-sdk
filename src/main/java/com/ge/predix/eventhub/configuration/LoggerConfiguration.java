package com.ge.predix.eventhub.configuration;

import com.ge.predix.eventhub.EventHubUtils;

import java.util.UUID;
import java.util.logging.Level;

import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.*;

/**
 * Created by williamgowell on 11/14/17.
 * This class stores the logger configuration parameters
 *
 */
public class LoggerConfiguration {
    private final UUID runtimeId;
    private boolean prettyPrintJsonLogs;
    private LogLevel utilLogLevel ;
    private boolean enableLogs;
    private boolean useJUL;
    private boolean utilEnableHTTPLogging;


    public enum LogLevel {
        /**
         * Log level enum for setting the log level of the client
         */
        SEVERE("severe", Level.SEVERE), //
        OFF("off", Level.OFF), //
        WARNING("warning", Level.WARNING), //
        INFO("info", Level.INFO), //
        FINE("fine", Level.FINE), //
        FINER("finer", Level.FINER), //
        ALL("all", Level.ALL), //
        FINEST("finest", Level.FINEST); //

        private final String text;
        private final Level level;

        LogLevel(final String text, final Level level) {
            this.text = text;
            this.level = level;
        }
        public Level getLevel(){
            return this.level;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    /**
     * Build a logger config using the default values of the builder
     * @return Default Logger Config
     */
    public static LoggerConfiguration defaultLogConfig(){
        return new Builder().build();
    }

    /**
     * Build a loggerConfig from a builder
     * @param builder the builder used to construct the logger config
     */
    private LoggerConfiguration(Builder builder){
        this.utilEnableHTTPLogging = builder.utilEnableHTTPLogging;
        this.enableLogs = builder.enableLogs;
        this.utilLogLevel = builder.utilLogLevel;
        this.useJUL = builder.useJUL;
        this.prettyPrintJsonLogs = builder.prettyPrintJsonLogs;
        this.runtimeId = UUID.randomUUID();
    }

    /**
     * Does the logging config enable HTTP Logging
     * @return this.enableHTTPLogging
     */
    public boolean utilLogHTTPLogging(){
        return this.utilEnableHTTPLogging;
    }

    /**
     * Does the logging confg have any logging enabled
     * @return enableLogs
     */
    public boolean loggingEnabled(){
        return this.enableLogs;
    }

    /**
     * Get the configured log level
     * @return Level
     */
    public Level getUtilogLevel(){
        return this.utilLogLevel.level;
    }

    /**
     * Should all logs be sent thouhg SLF4J
     * @return useJUL
     */
    public boolean useJUL(){
        return this.useJUL;
    }

    /**
     * are logs currently pretty printed?
     * @return prettyPrintJsonLogs
     */
    public boolean isPrettyPrintJsonLogs(){
        return this.prettyPrintJsonLogs;

    }

    public UUID getRuntimeId(){
        return runtimeId;
    }



    /**
     * Builder object for creating a LoggingConfig
     */
    public static class Builder{
        private boolean prettyPrintJsonLogs = false;
        private LogLevel utilLogLevel = LogLevel.WARNING;
        private boolean enableLogs = true;
        private boolean useJUL = false;
        private boolean utilEnableHTTPLogging = false;

        /**
         * populate config from environment variables
         * @return Builder
         */
        public Builder fromEnvironmentVariables(){
            if(System.getenv(ENABLE_LOGS) != null){
                this.enableLogs =  System.getenv(ENABLE_LOGS).equalsIgnoreCase("true");
            }

            if(System.getenv(PRETTY_PRINT_LOG) != null){
                this.prettyPrintJsonLogs = System.getenv(PRETTY_PRINT_LOG).equalsIgnoreCase("true");
            }

            if(System.getenv(USE_JUL) != null){
                this.useJUL = System.getenv(USE_JUL).equalsIgnoreCase("true");
            }

            if(System.getenv(UTIL_HTTP_LOGGING) != null){
                this.utilEnableHTTPLogging = System.getenv(UTIL_HTTP_LOGGING).equalsIgnoreCase("true");
            }

            String strLevel = System.getenv(DEBUG_LEVEL);
            if(strLevel != null){
                switch (strLevel.toLowerCase()) {
                    case "off":
                        this.utilLogLevel = LogLevel.OFF;
                        break;
                    case "info":
                        this.utilLogLevel = LogLevel.INFO;
                        break;
                    case "fine":
                        this.utilLogLevel = LogLevel.FINE;
                        break;
                    case "finer":
                        this.utilLogLevel = LogLevel.FINER;
                        break;
                    case "finest":
                        this.utilLogLevel = LogLevel.FINEST;
                        break;
                    case "all":
                        this.utilLogLevel = LogLevel.ALL;
                        break;
                    case "severe":
                        this.utilLogLevel = LogLevel.SEVERE;
                        break;
                    default:
                        this.utilLogLevel = LogLevel.WARNING;
                        break;
                }
            }
            return this;
        }

        /**
         * Set pretty print logs value in builder
         * @param value the value to pretty print logs
         * @return Builder
         */
        public Builder prettyPrintJsonLogs(boolean value){
            this.prettyPrintJsonLogs = value;
            return this;
        }

        /**
         * Set the log level for the builder
         * @param level the LogLevel to use
         * @return Builder
         */
        public Builder logLevel(LogLevel level){
            this.utilLogLevel = level;
            return this;
        }

        /**
         * Set the enable all logs value of the builder
         * @param value the value to set
         * @return Builder
         */
        public Builder enableLogs(boolean value){
            this.enableLogs = value;
            return this;
        }


        /**
         * Enable HTTP logging
         * @param value the value to set
         * @return
         */
        public Builder utilEnableHTTPLogging(boolean value){
            this.utilEnableHTTPLogging = value;
            return this;
        }

        public Builder useJUL(boolean value){
            this.useJUL = value;
            return this;
        }

        public LoggerConfiguration build(){
            return new LoggerConfiguration(this);
        }

    }

    public Builder cloneConfig(){
        return new Builder()
                .utilEnableHTTPLogging(this.utilEnableHTTPLogging)
                .useJUL(this.useJUL)
                .prettyPrintJsonLogs(this.prettyPrintJsonLogs)
                .logLevel(this.utilLogLevel);
    }
    public String toString(){
        return EventHubUtils.formatJson(
                "enable_logging", this.enableLogs,
                "util_http_logging", this.utilEnableHTTPLogging,
                "utilLogLevel", this.utilLogLevel.text,
                "prettyPrintLogs", this.prettyPrintJsonLogs).toString();
    }
}
