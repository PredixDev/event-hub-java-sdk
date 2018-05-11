package com.ge.predix.eventhub;

import com.ge.predix.eventhub.client.*;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import com.ge.predix.eventhub.configuration.LoggerConfiguration;
import com.ge.predix.eventhub.configuration.PublishConfiguration;
import com.ge.predix.eventhub.configuration.SubscribeConfiguration;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.ArrayList;
import java.util.Date;
import java.util.logging.*;

import static com.ge.predix.eventhub.EventHubConstants.EXCEPTION_KEY;
import static com.ge.predix.eventhub.EventHubConstants.MSG_KEY;
import static com.ge.predix.eventhub.EventHubUtils.formatJson;

/**
 * Created by williamgowell on 11/14/17.
 * This is a warpper around slf4j and java.util.logging. Each class has its own instance
 * of one of these loggers. The biggest key is that the log statements are no constucted if the
 * log level is not enabled.
 */
public class EventHubLogger {

    private static int  severeLogLevel = Level.SEVERE.intValue();
    private static int  warningLogLevel = Level.WARNING.intValue();
    private static int  infoLogLevel = Level.INFO.intValue();
    private static int  fineLogLevel = Level.FINE.intValue();
    private static int  finerLogLevel = Level.FINER.intValue();
    private static int  finestLogLevel = Level.FINEST.intValue();

    private LoggerConfiguration loggerConfiguration;
    private Class logClass;
    //If we are logging though java.util then we want to save the logger or else it will be garbage collected and reset
    private Object logger;

    /**
     * Construct a logger with a class and a given EventHubConfiguration
     * @param c the class to assign the logger to
     * @param configuration the EventHubConfiguration to configure the logger with
     */
    public EventHubLogger(Class c, EventHubConfiguration configuration){
        this(c, configuration.getLoggerConfiguration());
    }

    /**
     * Construct a EventHubLogger with a class and a loggerConfiguration
     * @param c the class to assin the logger to
     * @param configuration LoggerConfiguration to configure the logger with.
     */
    public EventHubLogger(Class c, LoggerConfiguration configuration){
        this.loggerConfiguration = configuration;
        this.logClass = c;

        if(loggerConfiguration.useJUL()){
            if(loggerConfiguration.utilLogHTTPLogging()){
                setHTTLLogging();
            }
            Logger logger = Logger.getLogger(c.getName());
            //logger.setUseParentHandlers(false);
            Handler[] handlers = logger.getHandlers();
            for (Handler handler : handlers) {
                logger.removeHandler(handler);
            }
            //tell the logger not to use the parent handlers
            //If the client needs to pipe logs to other places
            // then they should be using slf4j
            logger.setUseParentHandlers(false);
            Handler newHandler = new ConsoleHandler();
            newHandler.setFormatter(new LogFormatter(loggerConfiguration));
            logger.addHandler(newHandler);
            logger.setLevel(loggerConfiguration.getUtilogLevel());
            this.logger = logger;
        }else{
            logger = LoggerFactory.getLogger(c);
        }
    }

    /**
     * This gets called for each logger created and only if JUL is enabled
     * If SLF4J is used then these can be configured in the proper logger.properties file.
     */
    private void setHTTLLogging() {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "DEBUG");
    }

    /**
     * All logs for the SDK are passed here
     * First decided if they need to be logged or not.
     *  if they need to be logged, the JSONObject for that log message is constructed and passed to the logAtLevel
     *
     * @param level the level to log at
     * @param values the values to be converted to JSON to then be logged
     */
    public void log(Level level, Object ... values){
        if(this.loggerConfiguration.loggingEnabled()) {
            if(this.loggerConfiguration.useJUL()){
                logThoughUtil(level, values);
            }else{
                logThoughSLF4J(level, values);
            }
        }
    }

    /**
     * Log a message though the JUL
     *
     * @param level the level to be logged at
     * @param values the values to be logged
     */
    private void logThoughUtil(Level level, Object ... values){
        Logger logger = (Logger) this.logger;
        if (level.intValue() == severeLogLevel){
            logger.severe(makeMessage(false, loggerConfiguration.getRuntimeId().toString(), values));
        }
        else if (level.intValue() == warningLogLevel){
            logger.warning(makeMessage(false, loggerConfiguration.getRuntimeId().toString(), values));
        }
        else if (level.intValue() == infoLogLevel){
            logger.info(makeMessage(false, loggerConfiguration.getRuntimeId().toString(), values));
        }
        else if (level.intValue() == fineLogLevel){
            logger.fine(makeMessage(false, loggerConfiguration.getRuntimeId().toString(), values));
        }
        else if (level.intValue() == finerLogLevel){
            logger.finer(makeMessage(false, loggerConfiguration.getRuntimeId().toString(), values));
        }
        else if (level.intValue() == finestLogLevel){
            logger.finest(makeMessage(false, loggerConfiguration.getRuntimeId().toString(), values));
        }
    }

    private void logThoughSLF4J(Level level, Object ... values){
        org.slf4j.Logger logger = (org.slf4j.Logger) this.logger;
        if(logger.isErrorEnabled() && level.intValue() == severeLogLevel){
            logger.error(makeMessage(loggerConfiguration.isPrettyPrintJsonLogs(), loggerConfiguration.getRuntimeId().toString(), values));
        }else if(logger.isWarnEnabled() && level.intValue() == warningLogLevel){
            logger.warn(makeMessage(loggerConfiguration.isPrettyPrintJsonLogs(), loggerConfiguration.getRuntimeId().toString(), values));
        }else if(logger.isInfoEnabled() && level.intValue() == infoLogLevel){
            logger.info(makeMessage(loggerConfiguration.isPrettyPrintJsonLogs(), loggerConfiguration.getRuntimeId().toString(), values));
        }else if(logger.isDebugEnabled() && level.intValue() == fineLogLevel){
            logger.debug(makeMessage(loggerConfiguration.isPrettyPrintJsonLogs(), loggerConfiguration.getRuntimeId().toString(), values));
        }else if(logger.isTraceEnabled()){
            logger.trace(makeMessage(loggerConfiguration.isPrettyPrintJsonLogs(), loggerConfiguration.getRuntimeId().toString(), values));
        }
    }

    private String makeMessage(boolean prettyPrint, String runtimeId, Object ... values){
        JSONObject json;
        if(values.length==0){
            json = new JSONObject();
            json.put(MSG_KEY, "null");
            json.put("runtime_id", runtimeId);
        }else if(values.length == 1){
            json = new JSONObject();
            json.put(MSG_KEY, values[0]);
            json.put("runtime_id", runtimeId);
        }else{
            json = EventHubUtils.formatJson(values);
            json.put("runtime_id", runtimeId);
        }
        if(prettyPrint){
            return json.toString(2);
        }else{
            return json.toString();
        }
    }

    private static final class LogFormatter extends SimpleFormatter {
        private LoggerConfiguration configuration;

        LogFormatter(LoggerConfiguration configuration){
            super();
            this.configuration = configuration;
        }
        @Override
        public synchronized String format(LogRecord record) {
            JSONObject json;
            if (record.getThrown() == null) {
                json = EventHubUtils.formatJson(
                        "event-hub-java-sdk",
                        "timestamp", new Date(record.getMillis()).toString(),
                        "level", record.getLevel().getLocalizedName(),
                        "class", record.getLoggerName(),
                        "log_message", formatMessage(record)
                );
            } else {
                json = EventHubUtils.formatJson(
                        "event-hub-java-sdk",
                        "timestamp", new Date(record.getMillis()).toString(),
                        "level", record.getLevel().getLocalizedName(),
                        "class", record.getLoggerName(),
                        "log_message", formatMessage(record),
                        EXCEPTION_KEY, record.getThrown()
                );
            }

            if (configuration.isPrettyPrintJsonLogs())
                return json.toString(2) + "\n";
            else {
                return json.toString() + "\n";
            }
        }
    }

}
