package com.ge.predix.eventhub.client;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.ge.predix.eventhub.Message;
import com.ge.predix.eventhub.client.Client;
/**
 * A Sample CustomMetrics Callback
 * @author krunal
 *
 */
public class CustomMetrics implements  Client.MetricsCallBack {
    private List<Message> messages = Collections.synchronizedList(new LinkedList<Message>());  // Metrics Messages that will arrive
    
   
    
    public CustomMetrics() {
        
    }
    /**
     * 
     * @return message count
     */
    public int messageCount() {
        return messages.size();
    }
    
    /**
     * Gets you all the metrics messages
     * @return
     */
    public List<Message> getMessages(){
        return this.messages;
    }
    
    @Override
    public void onMessage(Message m) {
        // TODO Auto-generated method stub
        System.out.println(" Message received from Metrics Callback "+m);
        messages.add(m);
    }
    
    @Override
    public void onFailure(Throwable throwable) {
        // TODO Auto-generated method stub
        System.out.println( " Got failure from Metrics Callback "+throwable.getMessage());
    }
}
