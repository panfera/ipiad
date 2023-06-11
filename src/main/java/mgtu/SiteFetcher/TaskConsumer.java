package mgtu.SiteFetcher;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.JsonMappingException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

public class TaskConsumer extends Thread {
    public static Logger log = LogManager.getLogger();
    private Channel channel;
    static String exchangeName = "";
    static String queueDownload = "queue download";
    static String queueParse = "queue parse";
    static String consumerTag = "myConsumerTag";

    static String queueElk = "elk_queue";
    static String routingKey_elastic = "Route_to_elastic";
    static String serverUrl = "https://www.sport-express.ru/";
    Connection conn;

    public TaskConsumer(RabbitMqCreds rabbitCreds) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitCreds.username);
        factory.setPassword(rabbitCreds.password);
        factory.setVirtualHost(rabbitCreds.virtualHost);
        factory.setHost(rabbitCreds.host);
        factory.setPort(rabbitCreds.port);
        this.conn = factory.newConnection();
        this.channel = this.conn.createChannel();
        this.channel.queueDeclare(queueDownload, false, false, false, null);
        //this.channel.queueDeclare(queueElk, false, false, false, null);
        publishToRMQ(serverUrl, queueDownload);
    }

    @Override
    public void run() {
        try {
            channel.basicConsume(queueParse, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, StandardCharsets.UTF_8);
                    List<String> urls = parseDocument(message);
                    log.info("Parsing new html" + urls);
                    for (String url_ : urls) {
                        log.info("Add to queueDownload new url: " + url_);
                        publishToRMQ(url_, queueDownload);
                    }
                    Article article = getArticle(message);
                    if (article!= null) {
                        // convert user object to json string and return it
                        String jsonString = article.convert_to_Json().toString();
                        publishToRMQ(jsonString, queueElk);
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

    public Article getArticle(String doc) {
        try {
            Document parsedDoc;
            String timeTag, title, url, author, content;
            SimpleDateFormat formatter;
            Elements contents;
            Date date;

            parsedDoc = Jsoup.parse(doc);

            try {
                timeTag = parsedDoc.getElementsByTag("time").first().attr("datetime");
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX", Locale.ENGLISH);
                date = formatter.parse(timeTag);
            }catch (Exception e){
                date = new Date();
            }

            try {
                title = parsedDoc.getElementsByTag("title").first().text();
            }catch(Exception e){
                title = "";
            }

            try {
                url = parsedDoc.select("meta[property=og:url]").first().attr("content");
            }catch(Exception e){
                url = "";
            }

            try{
                author = parsedDoc.getElementsByClass("author-item").first().select("a").first().text();
            }catch(Exception e){
                author = "";
            }

            try {
                contents = parsedDoc.getElementsByClass("b_article-text").select("p");
                content = "";
                for (Element element : contents) {
                    content += element.text() + "\n";
                }
            }catch(Exception e){
                content="";
            }
            return new Article(title, author, url, date, content);
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }

    public List<String> parseDocument(String doc) {
        List<String> urls = new ArrayList();
        try {
            Document parsedDoc = Jsoup.parse(doc);
            Elements aTag = parsedDoc.getElementsByClass("se-mainnews-item"); //.getElementsByClass("w_col2"). select("a");
            for (Element element : aTag) {
                try {
                    String link = element.attr("data-url");
//                    log.info(element.text());
                    if (!link.startsWith("https://") && !link.startsWith("http://")) {
                        link = serverUrl + link;
                    }
                    System.out.printf("link: %s \n", link);
                    urls.add(link);
                } catch (Exception e) {
                    log.error(e);
                }
            }
        } catch (Exception ex) {
            log.error(ex);
        }
        return urls;
    }
}
