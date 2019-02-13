package com.ge.predix.eventhub.client;
/**
 * A base class to raise eventhub alarms. 
 * 
 * @author krunal
 *
 */
public class EventHubAlarm {

    private String message;     // alarm message
    private TYPE type;          // type of alarm
        
    public enum TYPE {
        RECONNECT
    }
        
    public EventHubAlarm(String message, TYPE type) {
        this.message = message;
        this.type = type;
    }
    
    public String logAlaram() {
        return new StringBuilder("EventHubAlarm : Alarm for ").append(this.type).append(" : ").append(this.message).toString();
    }
}
