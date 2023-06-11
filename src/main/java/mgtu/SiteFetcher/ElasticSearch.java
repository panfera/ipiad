package mgtu.SiteFetcher;

import com.rabbitmq.client.*;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
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
    private static final String mapping = """
            {
              "mappings": {
                "properties": {
                 "title": {
                    "type": "text",
                    "analyzer": "russian"
                  },
                  "author": {
                    "type": "text",
                    "analyzer": "russian"
                  },
                  "url": {
                    "type": "keyword"
                  },
                  "date": {
                    "type": "date"
                  },
                  "content": {
                    "type": "text",
                    "analyzer": "russian"
                  },
                  "sha256": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;

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


        GetIndexRequest request_check = new GetIndexRequest(index);

        boolean exists = client.indices().exists(request_check, RequestOptions.DEFAULT);

        if (exists){
            log.info("Index already exists");
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        request.mapping(
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"title\": {\n" +
                        "      \"type\": \"text\",\n" +
                        "      \"analyzer\": \"russian\"\n" +
                        "    },\n" +
                        "    \"author\": {\n" +
                        "      \"type\": \"text\",\n" +
                        "      \"analyzer\": \"russian\"\n" +
                        "    },\n" +
                        "    \"url\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    },\n" +
                        "    \"date\": {\n" +
                        "      \"type\": \"date\"\n" +
                        "    },\n" +
                        "    \"content\": {\n" +
                        "      \"type\": \"text\",\n" +
                        "      \"analyzer\": \"russian\"\n" +
                        "    },\n" +
                        "    \"sha256\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            if (createIndexResponse.isAcknowledged()) {
                log.info("Success create index");
            }else{
                log.info("IndexResponse: " + createIndexResponse.toString());
            }
        } catch (InvalidIndexNameException e) {
            log.error(e);
        }
    }

    public static boolean elk_check_unique(Article article) throws IOException {
        GetRequest request = new GetRequest(index, article.sha256);
        boolean exist = client.exists(request, RequestOptions.DEFAULT);
        return exist;
    }
    private void push_to_elk(Article obj) throws IOException {
        IndexRequest request = new IndexRequest(index);

        request.id(obj.sha256);
        request.source(obj.convert_to_Json(), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String index = response.getIndex();

        String id = response.getId();

        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("Success push to elk, id: " + id);
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("Success update, id: " + id);
        }
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
                        push_to_elk(article);
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

