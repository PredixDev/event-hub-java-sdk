/*
* Copyright (c) 2016 GE. All Rights Reserved.
* GE Confidential: Restricted Internal Distribution
*/
package com.ge.predix.eventhub;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.ge.predix.eventhub.configuration.EventHubConfiguration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import static com.ge.predix.eventhub.EventHubConstants.*;

/**
 * H
 */
public class EventHubUtils {

    private static final String protocolGRPC = "grpc";
    // this map caches the instance details based on it's name so we don't have to search through VCAP_SERVICES again
    private static ConcurrentHashMap<String, JsonNode> instanceFromConfiguration = new ConcurrentHashMap<String, JsonNode>();


    public static EventHubConfiguration getEventHubDetailsFromVCAPS(String vcapServices, String serviceName) throws EventHubClientException.InvalidConfigurationException {

        JsonNode eventHubInstance = findByServiceName(vcapServices, serviceName);

// Currently publish and subscribe details for Event Hub instance will be the same
        JsonNode protocols = eventHubInstance.get("credentials").get("publish").get("protocol_details");
        JsonNode grpcDetails = null;
// find grpc details
        Iterator<JsonNode> protocolsItr = protocols.iterator();
        while (protocolsItr.hasNext()) {
            JsonNode protocol = protocolsItr.next();
            if (protocol.get("protocol").asText().equals(protocolGRPC)) {
                grpcDetails = protocol;
                break;
            }
        }

        if (grpcDetails == null) {
            throw new EventHubClientException.InvalidConfigurationException(String
                    .format("Could not find grpc protocol details for Event Hub Service Instance with name: %s", serviceName));
        }

        String zoneID = eventHubInstance.get("credentials").get("publish").get("zone-http-header-value").asText();
        String URI = grpcDetails.get("uri").asText();

        if (URI.isEmpty()) {
            throw new EventHubClientException.InvalidConfigurationException("Could not fine GRPC URI in VCAP_SERVICES");
        }
        if (zoneID.isEmpty()) {
            throw new EventHubClientException.InvalidConfigurationException("Could not fine GRPC zoneID in VCAP_SERVICES");
        }

        EventHubConfiguration EHPartialConfig = new EventHubConfiguration.Builder()
                .host(getHostFromURL(URI))
                .port(getPortFromURL(URI))
                .zoneID(zoneID)
                .automaticTokenRenew(false)   // required to build config without auth details
                .build();

        return EHPartialConfig;
    }

    public static String getUAADetailsFromVCAPS(String vcapServices, String serviceName) throws EventHubClientException.InvalidConfigurationException {
        JsonNode uaaInstance = findByServiceName(vcapServices, serviceName);
        return uaaInstance.get("credentials").get("issuerId").asText();
    }

    public static String getValueFromEnv(String envName) throws EventHubClientException.InvalidConfigurationException {
        String value = System.getenv(envName);
        String error = String.format("%s environment variable", envName);
        return checkNotNull(value, error);
    }

    public static String checkNotNull(String value, String valueName) throws EventHubClientException.InvalidConfigurationException {
        if (value == null || value.isEmpty()) {
            String error = String.format("%s is not set", valueName);
            throw new EventHubClientException.InvalidConfigurationException(error);
        }
        return value;
    }

    public static JsonNode findByServiceName(String vcapServices, String serviceName) throws EventHubClientException.InvalidConfigurationException {
        if (instanceFromConfiguration.containsKey(serviceName)) {
            return instanceFromConfiguration.get(serviceName);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json;
        try {
            json = mapper.readTree(vcapServices.getBytes());
        } catch (Exception e) {
            throw new EventHubClientException.InvalidConfigurationException("Could not parse VCAP_SERVICES. Perhaps it is not set?");
        }

        try {
// loop through services
            Iterator<JsonNode> serviceItr = json.iterator();
            while (serviceItr.hasNext()) {
                JsonNode service = serviceItr.next();
// loop through service instances
                Iterator<JsonNode> instanceItr = service.iterator();
                while (instanceItr.hasNext()) {
                    JsonNode instance = instanceItr.next();
                    if (instance.get("name").asText().equals(serviceName)) {
                        instanceFromConfiguration.put(serviceName, instance);
                        return instance;
                    }
                }
            }
        } catch (Exception e) {
            throw new EventHubClientException.InvalidConfigurationException(String.format("Error finding instance with name: %s", serviceName));
        }
        throw new EventHubClientException.InvalidConfigurationException(String.format("Could not find instance with name: %s", serviceName));
    }

    private static String getHostFromURL(String url) {
        String[] parts = url.split(":");
        return parts[0];
    }

    private static int getPortFromURL(String url) {
        String[] parts = url.split(":");
        return Integer.parseInt(parts[1]);
    }

    public static boolean checkErrorAllowed(Throwable thrown, Throwable expected) {
        return thrown.getCause() != null && thrown.getCause().equals(expected);
    }

    /**
     * insert the key->value pairs into a json
     * If the value is of certain types then use the make json functionality
     * @param jsonObj the json object to be worked on
     * @param key the (str) key of the value
     * @param value the object of interest to be placed into the value
     */
    private static void putIntoJson(JSONObject jsonObj, String key, Object value){
        if(value instanceof JSONArray || value instanceof  JSONObject){
            jsonObj.put(key, value);
            return;
        }
        if(value instanceof PublishResponse) {
            jsonObj.put(key, makeJson((PublishResponse) value));
            return;
        }
        if(value instanceof List && ((List) value).size()>0 && ((List) value).get(0) instanceof Message){
            jsonObj.put(key, makeJson((List<Message>) value));
            return;
        }
        if(value instanceof  Message){
            jsonObj.put(key, makeJson((Message) value));
            return;
        }
        if(value instanceof Ack){
            jsonObj.put(key, makeJson((Ack) value));
            return;
        }
        if(value instanceof Throwable){
            jsonObj.put(key, makeJson((Throwable) value));
            return;
        }

        if(value instanceof String && ((String) value).length() > 0){
            try {
                if (((String) value).charAt(0) == '[') {
                    jsonObj.put(key, new JSONArray(new JSONTokener((String) value)));
                } else if (((String) value).charAt(0) == '{') {
                    jsonObj.put(key, new JSONObject(new JSONTokener((String) value)));
                } else {
                    jsonObj.put(key, value);
                }
            }catch (JSONException e){
                jsonObj.put(key, value);
            }
            return;
        }
        if(value instanceof List){
            JSONArray array = new JSONArray();
            for(Object o : (List) value){
                array.put(o);
            }
            jsonObj.put(key, new JSONArray(value));
            return;
        }
        jsonObj.put(key, value);
    }

    /**
     * turn a GRPC PublishResponse into a JSONArray of AcKs in the from of JSONObjecsts
     * @param publishResponse the publish response to be converted
     * @return JSONArray
     */
    public static JSONArray makeJson(PublishResponse publishResponse){
        JSONArray acks  = new JSONArray();
        for(Ack a: publishResponse.getAckList()){
            acks.put(makeJson(a));
        }
        return acks;
    }

    /**
     * Turn the GRPC Message into a JSONObject
     * @param m message to be converted to JSON
     * @return JSONObject
     */
    public static JSONObject makeJson(Message m){
        JSONObject message = new JSONObject();
        message.put("id", m.getId());
        message.put("body", m.getBody().toStringUtf8());
        message.put("topic", m.getTopic());
        message.put("partition", m.getPartition());
        message.put("offset", m.getOffset());
        message.put("zoneId", m.getZoneId());
        return message;
    }

    /**
     * Turn a list of GRPC messages into a JSONArray
     * @param messages the list to be converted to a JSONArray
     * @return JSONArray
     */
    public static JSONArray makeJson(List<Message> messages){
        JSONArray jsonMessages = new JSONArray();
        for(Message m : messages){
            jsonMessages.put(makeJson(m));
        }
        return jsonMessages;
    }

    public static JSONArray makeJson(SubscriptionMessage subscriptionMessage){
        return makeJson(subscriptionMessage.getMessages().getMsgList());
    }

    /**
     * Turn a GRPC Ack object into a JSONObject
     * @param a Ack to be converted into a json object
     * @return JSONObject
     */
    public static JSONObject makeJson(Ack a){
        JSONObject ackJson = new JSONObject();
        ackJson.put("id", a.getId());
        ackJson.put("partition", a.getPartition());
        ackJson.put("offset", a.getOffset());
        ackJson.put("topic", a.getTopic());
        ackJson.put("zoneId", a.getZoneId());
        ackJson.put("status", a.getStatusCode().toString());
        return ackJson;
    }

    /**
     * Turn a Throwable into a json
     * @param t throwable to be converted into a JSONObject
     * @return JSONObject
     */
    public static JSONObject makeJson(Throwable t){
        JSONObject throwableJson = new JSONObject();
        throwableJson.put(EXCEPTION_NAME_KEY, t.getClass());
        putIntoJson(throwableJson, EXCEPTION_MSG_KEY, t.getMessage());
        if(t.getStackTrace().length >= 3)
            throwableJson.put(EXCEPTION_STACK_TRACE_KEY, new JSONArray(Arrays.copyOfRange(t.getStackTrace(), 0, 2)));
        else{
            throwableJson.put(EXCEPTION_STACK_TRACE_KEY, new JSONArray(t.getStackTrace()));
        }
        return throwableJson;
    }

    /**
     * Take in a list of objects and format them as a JSONObject
     * if even number of values is supplied:
     *  {value0:value1, value2:value3}
     *
     *  if odd number, it will use the first value as a key for a nesed obejct
     *  {value0: value1: value2, value3: value4}
     * @param values values list to be converted into a JSONObject
     * @return JSONObject
     */
    public static JSONObject formatJson(Object ... values){
        JSONObject json = new JSONObject();
        int startIndex = values.length % 2;
        if(startIndex == 1 ){
            JSONObject innerJson = new JSONObject();
            for(int i=1; i<values.length;i+=2){
                putIntoJson(innerJson, values[i].toString(), values[i+1]);
            }
            json.put(String.valueOf(values[0]), innerJson);
        }else{
            for(int i=0;i<values.length;i+=2){
                putIntoJson(json, values[i].toString(), values[i+1]);
            }
        }
        return json;
    }
}
