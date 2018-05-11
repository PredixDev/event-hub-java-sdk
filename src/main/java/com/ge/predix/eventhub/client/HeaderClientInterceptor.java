package com.ge.predix.eventhub.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import com.ge.predix.eventhub.EventHubConstants;
import com.ge.predix.eventhub.EventHubLogger;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import com.ge.predix.eventhub.EventHubClientException;
import com.ge.predix.eventhub.EventHubUtils;
import com.ge.predix.eventhub.configuration.EventHubConfiguration;

import static com.ge.predix.eventhub.EventHubConstants.*;
import static com.ge.predix.eventhub.EventHubConstants.EnvironmentVariables.SCOPE_PREFIX;
import static com.ge.predix.eventhub.EventHubConstants.HeaderClientInterceptorConstants.*;

/**
 * A interceptor to handle client header.
 * The majority of this class handles getting the token and managing the scopes
 * see interceptCall() for the actual intercepting of the scopes
 *
 */
class HeaderClientInterceptor implements ClientInterceptor {
    protected String scopePrefix = "predix-event-hub.zones.";

    private String uaaToken;
    private long tokenExpirationTime;
    private String scopes;

    protected final String client_authorization;
    protected final EventHubConfiguration configuration;
    protected EventHubLogger ehLogger;
    protected final Client client;
    private long validSafteyInterval = 1000;

    /**
     * Make the headerClientInterceptor
     * @param client the  who made the interceptor so we can throw errors to sub-streams on bad auth
     * @param configuration the configuration to pull information from (auth)
     */
    HeaderClientInterceptor(Client client, EventHubConfiguration configuration) {
        this.ehLogger = new EventHubLogger(this.getClass(), configuration);
        this.client = client;
        this.configuration = configuration;
        this.client_authorization =
                "Basic " + new String(Base64.encodeBase64((configuration.getClientID() + ":" + configuration.getClientSecret()).getBytes()));
        this.uaaToken = "";
        this.tokenExpirationTime = System.currentTimeMillis() - 50;
        scopePrefix = System.getenv(SCOPE_PREFIX) != null && !System.getenv(SCOPE_PREFIX).isEmpty() ? System.getenv(SCOPE_PREFIX) : scopePrefix;
    }

    /**
     * Set the auth token
     * @param uaaToken the token to be set
     * @param cause what caused the set auth
     */
    protected synchronized void setAuthToken(String uaaToken, String cause) {
        this.uaaToken = uaaToken;
        ehLogger.log( Level.FINE,
                INTERCEPTOR_MSG,
                MSG_KEY, "token has been set",
                "token", uaaToken,
                CAUSE_KEY, cause,
                FUNCTION_NAME_STRING, "HeaderClientInterceptor.setAuthToken"
        );
    }

    /**
     * Return the auth token
     * @return the current auth token
     */
    protected synchronized String getAuthToken() {
        return this.uaaToken;
    }

    /**
     * Force the token to be updated
     * @throws EventHubClientException if something happens while receiving the token
     */
    protected synchronized void forceRenewToken() throws EventHubClientException {
        this.setAuthToken(this.getToken(), "forceRenewToken");
    }

    /**
     * generate the scopes and retrieve the token from uaa
     * @return Token got from uaa
     * @throws EventHubClientException
     */
    private synchronized String getToken() throws EventHubClientException.AuthTokenRequestException, EventHubClientException.AuthenticationException {
        if(this.scopes == null){
            if(this.configuration.getAuthScopes() != null){
                this.scopes = configuration.getAuthScopes();
            }
            else{
                this.scopes  = generateScopes();
            }
        }
        ehLogger.log( Level.INFO,
                INTERCEPTOR_MSG,
                MSG_KEY, "token renewing",
                "auth URL", configuration.getAuthURL(),
                "scopes" , scopes
        );
        return this.requestToken();
    }

    /**
     * Setup and make the request to the AuthURI
     * also set the validity time
     * @return oAuth token if
     * @throws EventHubClientException.AuthenticationException
     * @throws EventHubClientException.AuthTokenRequestException
     */
    private synchronized String requestToken() throws EventHubClientException.AuthenticationException, EventHubClientException.AuthTokenRequestException {

        String headerContentTypeKey  = "content-type";
        String headerAuthorizationKey = "authorization";
        String authResponseAccessTokenKey = "access_token";
        String authResponseExpiresKey = "expires_in";

        String urlEncodedParams = String.format("grant_type=%s&response_type=%s&scope=%s", "client_credentials", "token", scopes);
        String proxy_uri = System.getenv().get(EventHubConstants.EnvironmentVariables.PROXY_URI);
        String proxy_port = System.getenv().get(EventHubConstants.EnvironmentVariables.PROXY_PORT);
        HttpClient httpClient;

        if (proxy_uri != null && proxy_port != null) {
            ehLogger.log( Level.INFO,
                    INTERCEPTOR_MSG,
                    FUNCTION_NAME_STRING, "HeaderClientInterceptor.requestToken",
                    MSG_KEY, "using proxy for token request",
                    "proxy", proxy_uri + ":" + proxy_port
            );
            httpClient = HttpClientBuilder.create()
                    .useSystemProperties()
                    .setProxy(new HttpHost(proxy_uri, Integer.parseInt(proxy_port), "http"))
                    .build();
        } else {
            httpClient = HttpClientBuilder.create()
                    .useSystemProperties()
                    .build();
        }

        String uaaToken;
        try {
            HttpPost request = new HttpPost(configuration.getAuthURL());
            StringEntity params = new StringEntity(urlEncodedParams);
            request.addHeader(headerContentTypeKey,  "application/x-www-form-urlencoded");
            request.addHeader(headerAuthorizationKey, client_authorization);
            request.setEntity(params);
            HttpResponse response = httpClient.execute(request);

            if (response != null) {
                InputStream in = response.getEntity().getContent();
                String encoding = "UTF-8";
                String body = IOUtils.toString(in, encoding);
                JSONObject resObj;

                try {
                    resObj = new JSONObject(body);
                } catch (JSONException e) {
                    throw new EventHubClientException.AuthTokenRequestException(EventHubUtils.formatJson(
                            INTERCEPTOR_ERR,
                            MSG_KEY,  "can't parse oAuth response into json",
                            FUNCTION_NAME_STRING, "HeaderClientInterceptor.requestToken",
                            "body",  body.equals("") ? "<Empty Response>" : body,
                            EXCEPTION_KEY,e
                    ).toString()    );
                }

                if(!resObj.has(authResponseAccessTokenKey) || !resObj.has(authResponseExpiresKey)){
                    throw new EventHubClientException.AuthenticationException(EventHubUtils.formatJson(
                            INTERCEPTOR_ERR,
                            MSG_KEY, "can't find token in response,  incorrect scopes or client info",
                            FUNCTION_NAME_STRING, "HeaderClientInterceptor.requestToken",
                            "body", body,
                            authResponseAccessTokenKey, resObj.has(authResponseAccessTokenKey),
                            authResponseExpiresKey, resObj.has(authResponseExpiresKey)
                    ).toString());
                }
                uaaToken = resObj.getString(authResponseAccessTokenKey);
                ehLogger.log( Level.FINEST,
                        INTERCEPTOR_MSG,
                        FUNCTION_NAME_STRING, "HeaderClientInterceptor.requestToken",
                        MSG_KEY, "got token",
                        "token", uaaToken);

                Long timeTillExpiration = resObj.getLong(authResponseExpiresKey);
                this.tokenExpirationTime = System.currentTimeMillis() + timeTillExpiration * 1000;
            } else {
                throw new EventHubClientException.AuthTokenRequestException(EventHubUtils.formatJson(
                        INTERCEPTOR_ERR,
                        MSG_KEY,  "response from oAuth2 provider was null",
                        FUNCTION_NAME_STRING, "HeaderClientInterceptor.requestToken"
                ).toString());
            }
        } catch (IOException e) {
            throw new EventHubClientException.AuthTokenRequestException(
                    EventHubUtils.formatJson(
                            INTERCEPTOR_ERR,
                            MSG_KEY, "Could not get token from OAuth2 provider",
                            FUNCTION_NAME_STRING, "HeaderClientInterceptor.requestToken",
                            EXCEPTION_KEY, e
                    ).toString());
        }
        return uaaToken;
    }


    /**
     * Generate the scopes for the configuration supplied by the client
     * It will always request root user scope. Only request the scopes for the clients provided
     * Subscribe Scopes:
     *  request the root scopes if topic list is empty, else request subscribe and user scope of that subtopic
     * Publish Scopes:
     *  request the roots scopes if topic is empty, else request publish and user(if not already added) of subtopic
     * @return comma delimited string containing the required scopes.
     */
    protected String generateScopes(){
        String publishScopePostfix =  ".grpc.publish";
        String subscribeScopePostfix = ".grpc.subscribe";
        String userScopePostfix = ".user";

        String delimiter = ",";
        StringBuilder scopes = new StringBuilder();
        scopes.append(scopePrefix);
        scopes.append(configuration.getZoneID());
        scopes.append(userScopePostfix);
        scopes.append(delimiter);
        if(configuration.getSubscribeConfiguration() != null){
            List<String> topics =  configuration.getSubscribeConfiguration().getTopics();
            if(topics.size() == 0){
                //we only have the default topic
                scopes.append(scopePrefix);
                scopes.append(configuration.getZoneID());
                scopes.append(subscribeScopePostfix);
                scopes.append(delimiter);
            }else{
                for(String topic : configuration.getSubscribeConfiguration().getTopics()){
                    scopes.append(scopePrefix).append(configuration.getZoneID()).append(".").append(topic).append(subscribeScopePostfix);
                    scopes.append(delimiter);
                }
            }
        }
        if(configuration.getPublishConfiguration() != null){
            String publishTopic = configuration.getPublishConfiguration().getTopic();
            if(publishTopic == null){
                scopes.append(scopePrefix).append(configuration.getZoneID()).append(publishScopePostfix);
                scopes.append(delimiter);
            }
            else{
                scopes.append(scopePrefix).append(configuration.getZoneID()).append(".").append(publishTopic).append(publishScopePostfix);
                scopes.append(delimiter);
            }
        }
        //remove last delimiter and return
        String strscopes = scopes.deleteCharAt(scopes.length()-1).toString();
        ehLogger.log( Level.INFO,
                INTERCEPTOR_MSG,
                MSG_KEY, "generated scopes",
                "scopes", strscopes);
        return strscopes;
    }

    /**
     * Test if the token is valid
     * use valid interval to provide a <1> second safety room
     * to prevent the edge case where the token is valid when we send it but
     * become invalid to service. (though this would just cause a reconnect anyways)
     * @return
     */
    private boolean tokenIsValid() {
        return this.tokenExpirationTime > System.currentTimeMillis() + validSafteyInterval;
    }

    /**
     * This is the acutual grpc channel interceptor
     * Each time a stub (stream/service) is created on Client.orginChannel this will get called each time. If a token has been set
     * by a previous intercept and is still valid it will use it, else it will request a new one.
     * @param method the remote method to be called.
     * @param callOptions the runtime options to be applied to this call.
     * @param next the channel which is being intercepted.
     * @return the call object for the remote operation, never {@code null}.
     */
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
              @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                  ehLogger.log( Level.FINE,
                        INTERCEPTOR_MSG,
                            MSG_KEY, "intercepting the client call to add auth information",
                            "call", method.getFullMethodName()
                );
                if (!tokenIsValid() && configuration.isAutomaticTokenRenew()) {
                    try {
                        setAuthToken(getToken(), "getToken for " + method.getFullMethodName());
                    } catch (EventHubClientException e) {
                        ehLogger.log( Level.WARNING,
                                INTERCEPTOR_ERR,
                                MSG_KEY,"error in intercepting client call for auth, sending error to stream",
                                "call", method.getFullMethodName(),
                                EXCEPTION_KEY,e
                        );
                        client.throwErrorToStream(method.getFullMethodName(), Status.Code.UNAUTHENTICATED, e.getMessage(), e);
                    }
                }
                if(uaaToken.equals("")){
                    EventHubClientException e = new EventHubClientException.AuthenticationException("no token set");
                    client.throwErrorToStream(method.getFullMethodName(), Status.Code.UNAUTHENTICATED, e.getMessage(), e);
                }else{
                    headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), uaaToken);
                    headers.put(Metadata.Key.of("predix-zone-id", Metadata.ASCII_STRING_MARSHALLER), configuration.getZoneID());
                    headers.put(Metadata.Key.of("content-type", Metadata.ASCII_STRING_MARSHALLER), "application/grpc");
                    SimpleForwardingClientCallListener clientCallListener = new SimpleForwardingClientCallListener<RespT>(responseListener) {
                    };
                    ehLogger.log( Level.FINE,
                            INTERCEPTOR_MSG,
                            MSG_KEY,  "successfully attached token to stream",
                            "call", method.getFullMethodName()
                    );
                    super.start(clientCallListener, headers);
                }
            }
        };
    }

}

