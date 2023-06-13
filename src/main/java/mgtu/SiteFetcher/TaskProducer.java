package mgtu.SiteFetcher;

import com.rabbitmq.client.*;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.RequestLine;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class TaskProducer extends Thread{
    private static Logger log = LogManager.getLogger();
    public static final String PEER_CERTIFICATES = "PEER_CERTIFICATES";
    private CloseableHttpClient client = null;
    private HttpClientContext context;
    //private HttpClientBuilder builder;
    private URL serverUrl = new URL("https://www.mk.ru/");
    private List<Header> headers = new ArrayList<>();

    private int retryDelay = 5 * 1000;
    private int retryCount = 2;
    //private Timeout metadataTimeout = Timeout.ofSeconds(30);
    private int metadataTimeout = 30 * 1000;
    private Channel channel;
    static String exchangeName = "";
    static String RoutingKeyToDownload = "routingKeyToDownload";
    public static String userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 YaBrowser/23.1.2.998 Yowser/2.5 Safari/537.36";

    static String queueDownload = "queue download";
    static String queueParse = "queue parse";
    static String consumerTag = "myConsumerTag";

    Connection conn;

    public TaskProducer(RabbitMqCreds rabbitCreds) throws IOException, TimeoutException {
        CookieStore httpCookieStore = new BasicCookieStore();
        client = HttpClients.custom().setUserAgent(this.userAgent).build();
        context = HttpClientContext.create();
        context.setCookieStore(httpCookieStore);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitCreds.username);
        factory.setPassword(rabbitCreds.password);
        factory.setVirtualHost(rabbitCreds.virtualHost);
        factory.setHost(rabbitCreds.host);
        factory.setPort(rabbitCreds.port);
        this.conn = factory.newConnection();
        this.channel = this.conn.createChannel();
        this.channel.queueDeclare(queueParse, false, false, false, null);

        /*if (!server.startsWith("https://") && !server.startsWith("http://"))
            server = "http://" + server;
        try {
            serverUrl = new URL(server);
        } catch (MalformedURLException e){
            log.error(e);
        }*/
        //headers.add(new BasicHeader(HttpHeaders.USER_AGENT, this.userAgent));
        //headers.add(new BasicHeader(HttpHeaders.HOST, serverUrl.getHost()));
        //headers.add(new BasicHeader(HttpHeaders.ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\""));
        //headers.add(new BasicHeader(HttpHeaders.ACCEPT_LANGUAGE, "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3"));
        //headers.add(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate, br"));
        //headers.add(new BasicHeader(HttpHeaders.CONNECTION, "keep-alive"));
        //headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "text/html;charset=UTF-8"));
        //headers.add(new BasicHeader("Sec-Fetch-Dest", "document"));
        //headers.add(new BasicHeader("Sec-Fetch-Mode", "navigate"));
        //headers.add(new BasicHeader("Sec-Fetch-Site", "none"));
        //headers.add(new BasicHeader("Sec-Fetch-User", "?1"));
        //headers.add(new BasicHeader("Upgrade-Insecure-Requests", "1"));
        //headers.add(new BasicHeader("Pragma", "no-cache"));
        //headers.add(new BasicHeader("Cache-Control", "no-cache"));

    }

    @Override
    public void run(){
        try {
            channel.basicConsume(queueDownload, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, StandardCharsets.UTF_8);
                    log.info("New link: " + message);
                    //URL url = new URL(message);
                    try {
                        String doc = getUrl(message);
                        //String doc = String.valueOf(d);
                        //if (doc != "null") {
                            log.info("Add to queueParse new doc with link: " + message);
                            publishToRMQ(doc, queueParse);
                        //}
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void publishToRMQ(String element, String queuePublish) {
        byte[] messageBodyBytes = element.getBytes();
        log.info("Publishing to queue: " + queuePublish);
        Channel channel;
        try {
            channel = this.conn.createChannel();
        } catch (IOException e) {
            log.error(e);
            return;
        }
        try {
//            channel.queueDeclare(queuePublish, false, false, false, null);
            channel.basicPublish(
                    exchangeName,
                    queuePublish,
                    false,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
        } catch (Exception e) {
            log.error(e);
        }
        try {
            channel.close();
        } catch (Exception e) {
            log.error(e);
        }
    }
public String getUrl(String  url) throws URISyntaxException{
        //String url = server + "/news/" + newsId;
        int code = 0;
        boolean bStop = false;
        String doc = null;
        URI uri;
        try {
            uri = new URI(serverUrl + url);
        } catch (Exception e) {
            log.error(e);
            return null;
        }

        for (int iTry = 0; iTry < retryCount && !bStop; iTry++){
            log.info("getting page from url " + url);
            //client = builder.build();
            //client = HttpClientBuilder.create().build();
            RequestConfig requestConfig = RequestConfig.custom()
                    //.setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                response = client.execute(request);
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    log.warn("error get url " + url + " code " + code);
                    bStop = true;
                }
                else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        doc = EntityUtils.toString(entity, "UTF-8");
                        break;
                    }
                    bStop = true;
                } else {
                    if (code == 403) {
                        log.warn("error get url " + url + " code " + code);
                        for (Header header : response.getHeaders(url)) {
                            Header eheader = headers.stream()
                                    .filter(h -> h.getName().equals(header.getName()))
                                    .findAny()
                                    .orElse(null);
                            if (eheader == null)
                                headers.add(header);
                        }
                    } else {
                        log.warn("error get url " + url + " code " + code);
                        response.close();
                        response = null;
                        client.close();
                        /*CookieStore httpCookieStore = new BasicCookieStore();
                        builder.setDefaultCookieStore(httpCookieStore);
                        client = builder.build();*/
                        int delay = retryDelay * 1000 * (iTry + 1);
                        log.info("wait " + delay / 1000 + " s...");
                        try {
                            Thread.sleep(delay);
                            continue;
                        } catch (InterruptedException ex) {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
        return doc;
    }
}
