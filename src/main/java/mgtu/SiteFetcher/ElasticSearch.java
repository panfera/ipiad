package mgtu.SiteFetcher;

import com.rabbitmq.client.*;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.client.RestClient;
public class ElasticSearch extends Thread {
    public static Logger log = LogManager.getLogger();
    private Channel channel;
    static String exchangeName = "";
    static String queueDownload = "queue download";
    static String queueParse = "queue parse";
    static String queueElk = "elk_queue";
    static String consumerTag = "myConsumerTag";
    static String routingKey_elastic = "Route_to_elastic";
    static String serverUrl = "https://www.mk.ru/";
    Connection conn;
    String hostname = "localhost";
    int port = 9200;
    String sheme = "http";
    String userName = "elastic";
    String password = "elastic";
    private static RestHighLevelClient client;
    private static final String index = "news";

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
                AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        RestClientBuilder restClientBuilder = RestClient
                .builder(new HttpHost(hostname, port, sheme))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        client = new RestHighLevelClient(restClientBuilder);

        /*
        GetIndexRequest request_check = new GetIndexRequest(index);

        boolean exists = client.indices().exists(request_check, RequestOptions.DEFAULT);

        if (exists){
            log.info("Index already exists");
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
        );
        request.source(
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
        }*/
    }

    public static boolean elk_check_unique(Article article) throws IOException {
        GetRequest request = new GetRequest(index, article.sha256);
        boolean exist = client.exists(request, RequestOptions.DEFAULT);
        return exist;
    }

    public static SearchHits get_text(String title) throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("title", title));
        sourceBuilder.from(0);
        sourceBuilder.size(1);
        //sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        log.info("get texts");

        return hits;
    }
    private void push_to_elk(Article obj) throws IOException, NoSuchAlgorithmException {
       /* IndexRequest request = new IndexRequest(index);
        request.id(obj.sha256);
        request.source(obj.convert_to_Json(), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String index = response.getIndex();

        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("Success push to elk");
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("Success update in elk");
        }*/

        IndexRequest request = new IndexRequest(index);
        request.source(obj.convert_to_HashMap());
        request.id(obj.sha256);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("Success push to elk");
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("Success update in elk");
        }
    }



    @Override
    public void run() {
        try {
            GetIndexRequest request = new GetIndexRequest(index);
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

                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (Exception e) {
            log.error(e);
        }
    }
}

