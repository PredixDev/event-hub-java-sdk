package com.ge.predix.eventhub.sample.websocket;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFactory;
import com.neovisionaries.ws.client.ProxySettings;
import org.json.JSONObject;
import org.json.JSONTokener;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * Created by williamgowell on 11/21/17.
 * This is a example to show how to publish messages in java
 *
 * This does not require the event hub SDK>
 * This method should only be used if the GRPC/HTTP2 is not available on the system
 * as GRPC/HTTP2 has much better preformance.
 * Enviorment variables:

 AUTH_URL=<auth_url>
 EVENTHUB_URI=<eventhub_host>
 EVENTHUB_PORT=<port>
 CLIENT_SECRET=<client_secret>
 CLIENT_ID=<client_id>
 ZONE_ID=<zone_id>


 */
public class WebSocketPublish {

    /**
     * used to build a SSL context from a provided certificate
     * this is used when connecting to event hub that requires a custom certificate
     * Not required to connect to a event hub service in cloud foundry
     * @param path: path of the cert
     * @return SSLContex
     * @throws CertificateException
     * @throws IOException
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    private static SSLContext getContextFromFile(String path) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = null;
        InputStream is = new FileInputStream(path);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);
        TrustManagerFactory tmf = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null); // You don't need the KeyStore instance to come from a file.
        ks.setCertificateEntry("caCert", caCert);
        tmf.init(ks);
        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);
        return sslContext;
    }


    /**
     * Request a token to be attached to the headers of the websocket
     *
     *
     * @param authURL:    uaa url
     * @param clientAuth: base64 encoding of client name and password (see above)
     * @param scopes:     scopes to be requseted
     * @return token
     */
    private static synchronized String requestToken(String authURL, String clientAuth, String scopes) {
        HttpsURLConnection connection = null;
        String grantType = "client_credentials";
        String responseType = "token";
        String contentTypeToken = "application/x-www-form-urlencoded";
        String accessTokenKey = "access_token";
        String expiresInKey = "expires_in";


        try {
            URL url = new URL(authURL);
            connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("content-type", contentTypeToken);
            connection.setRequestProperty("Authorization", clientAuth);
            connection.setUseCaches(false);
            connection.setDoOutput(true);

            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            String data = String.format("grant_type=%s&response_type=%s&scope=%s", grantType, responseType, scopes);
            System.out.println("data = "+data);
            wr.writeBytes(data);

            InputStream is = connection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuffer response = new StringBuffer();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            reader.close();
            System.out.println("response : "+response.toString());
            JSONObject uaaResponse = new JSONObject(new JSONTokener( response.toString()));
            return uaaResponse.getString(accessTokenKey);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return null;
    }

    /**
     * Get a env variable and make it required
     * @param key the ket of the env
     *
     * @return the system environment variable
     */
    private static String getEnv(String key){
        return getEnv(key, true);
    }

    /**
     * Get a system env, throw error if not found
     * @param key: the key of the env
     * @param required should an error be thrown if the key is not found
     * @return the system enviroment variable
     */
    private static String getEnv(String key, boolean required){
        if(required && System.getenv(key) == null){
            throw new NoSuchElementException( key + " environment variable not set");
        }
        return System.getenv(key);
    }

    /**
     * Start up the web socket connection.
     * attach headers and register callback to print the messages recieved.
     * @param host: host of event hub service
     * @param port: port of event hub
     * @param zoneid: zone id of eventhub
     * @param token: the token received from uaa to authenticate with service
     * @return connected WebSocket Object
     */
    private static WebSocket initWebSocket(String host, int port, String zoneid,  String token){

        WebSocketFactory factory = new WebSocketFactory();
        if (getEnv("PROXY_URI",false) != null) {
            ProxySettings settings = factory.getProxySettings();
            settings.setServer(getEnv("PROXY_URI",false));
        }
        if (System.getenv("TLS_PEM_FILE") != null) {
            try {
                factory.setSSLContext(getContextFromFile(System.getenv("TLS_PEM_FILE")));
            } catch (CertificateException | IOException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
                e.printStackTrace();
            }
        }

        try {
            return factory
                    .createSocket(String.format("wss://%s:%d/v1/stream/messages/", host, port))
                    .addHeader("Authorization", token)
                    .addHeader("Predix-Zone-Id", zoneid)
                    .setPingInterval(30 * 1000)
                    .addListener(new WebSocketAdapter() {
                        @Override
                        public void onBinaryMessage(WebSocket ws, byte[] binary) {
                            System.out.println(new String(binary));
                        }
                    })
                    .connect();
        } catch (WebSocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Make the json string object
     * the body of this object is also a json object because when event hub servive the stores the body into kafak, the
     * substring is extracted exactly as it appears in the body field of this object.
     * if the body filed contained a string, the surrounding " would be included in the message
     *
     * a tag to tell that this message was also published as a web socket is also added, but not required
     *
     * a Tag is also added to the message
     *
     * @param id: the id of this message
     * @param body: the body of the message
     * @return JSON as string
     */
    private static String makeMessage(String id, String body){
        return String.format("[{\"id\":\"%s\", \"body\":{\"message\":\"%s\"}, \"tags\":{\"publish_type\":\"web_socket\"}}]", id, body);

    }

    public static void main(String args[]) {

        String host = getEnv("EVENTHUB_URI");
        int port  = getEnv("EVENTHUB_PORT", false) == null ? 443 : Integer.parseInt(getEnv("EVENTHUB_PORT", false));
        String zoneID = getEnv("ZONE_ID");
        String authURL = getEnv("AUTH_URL");
        String clientID = getEnv("CLIENT_ID");
        String clientSecret = getEnv("CLIENT_SECRET");

        String auth = "Basic " + Base64.getEncoder().encodeToString((clientID + ":" + clientSecret).getBytes());
        String scopes = String.format("predix-event-hub.zones.%s.wss.publish predix-event-hub.zones.%s.user", zoneID, zoneID);

        String token = requestToken(authURL, auth, scopes);
        System.out.println("Token: " + token);

        WebSocket socket = initWebSocket(host, port, zoneID, token);

        try{
            for (int i = 0; i < 10; i++) {
                String str = makeMessage("id-"+i, "message_body");
                System.out.println("Message : "+str);
                socket.sendBinary(str.getBytes());
                Thread.sleep(500L);
            }

            //wait for acks
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        socket.disconnect();
    }
}
