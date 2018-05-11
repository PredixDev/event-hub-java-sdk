package com.ge.predix.eventhub.sample.stomp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

/**
 * Class that configures the server socket endpoints
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/eventhub")
                .setAllowedOrigins("*")
                .withSockJS()
                .setHeartbeatTime(1000L);
    }

    @Bean
    public StompConnectEvent webSocketConnectHandler() {
        return new StompConnectEvent();
    }

    @Bean
    public StompDisconnectEvent webSocketDisconnectHandler() {
        return new StompDisconnectEvent();
    }

    class StompConnectEvent implements ApplicationListener<SessionConnectEvent> {
        private final Log logger = LogFactory.getLog(StompConnectEvent.class);
        @Override
        public void onApplicationEvent(SessionConnectEvent event) {
            StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
            EventHubClient.clientConnect();
            logger.debug("Connect event [sessionId: " + sha.getSessionId() +"]");
        }
    }

    class StompDisconnectEvent implements ApplicationListener<SessionDisconnectEvent> {

        private final Log logger = LogFactory.getLog(StompConnectEvent.class);
        @Override
        public void onApplicationEvent(SessionDisconnectEvent event){
            StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
            EventHubClient.clientDisconnect();
            logger.debug("Disconnect event [sessionId: " + sha.getSessionId() +"]");

        }

    }




}