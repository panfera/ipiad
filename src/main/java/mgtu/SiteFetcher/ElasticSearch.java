package mgtu.SiteFetcher;

import com.rabbitmq.client.*;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.json.JSONObject;
public class ElasticSearch extends Thread {
    public static Logger log = LogManager.getLogger();
    private Channel channel;
    static String exchangeName = "";
    static String queueDownload = "queue download";
    static String queueParse = "queue parse";
    static String queueElk = "elk_queue";
    static String consumerTag = "myConsumerTag";
    static String routingKey_elastic = "Route_to_elastic";
    static String serverUrl = "https://www.sport-express.ru/";
    Connection conn;
    String hostname = "localhost";
    int port = 9200;
    private static RestHighLevelClient client;
    private static final String index = "sport";

    public ElasticSearch(RabbitMqCreds rabbitCreds) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitCreds.username);
        factory.setPassword(rabbitCreds.password);
        factory.setVirtualHost(rabbitCreds.virtualHost);
        factory.setHost(rabbitCreds.host);
        factory.setPort(rabbitCreds.port);
        this.conn = factory.newConnection();
        this.channel = this.conn.createChannel();
        this.channel.queueDeclare(queueElk, false, false, false, null);
        /*RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));*/

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials("elastic", "elastic"));
        RestClientBuilder restClientBuilder = RestClient
                .builder(new HttpHost("localhost", 9200, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        client = new RestHighLevelClient(restClientBuilder);
    }

    private void push_to_elk(JSONObject obj) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);
        log.info("object: "+ obj);
        request.mapping(String.valueOf(obj), XContentType.JSON);
    }
    @Override
    public void run() {
        try {
            GetIndexRequest request = new GetIndexRequest("sport");
            boolean tmp = false;
            tmp = client.indices().exists(request, RequestOptions.DEFAULT);
            channel.basicConsume(queueElk, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, StandardCharsets.UTF_8);

                    log.info("New json object in elk");

                    try {
                        Article article = new Article(message);
                        push_to_elk(article.convert_to_Json());
                    } catch (Exception e) {
                        log.error(e);
                    }
                    // is_uniq? and send_to_elastic
                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (Exception e) {
            log.error(e);
        }
    }
}

