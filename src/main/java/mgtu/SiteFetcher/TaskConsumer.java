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

import static mgtu.SiteFetcher.ElasticSearch.elk_check_unique;

public class TaskConsumer extends Thread {
    public static Logger log = LogManager.getLogger();
    private Channel channel;
    static String exchangeName = "";
    static String queueDownload = "queue download";
    static String queueParse = "queue parse";
    static String consumerTag = "myConsumerTag";

    static String queueElk = "elk_queue";
    static String routingKey_elastic = "Route_to_elastic";
    static String serverUrl = "https://www.mk.ru/";
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

                    Article article = getArticle(message);
                    if (article!= null ) {//&& elk_check_unique(article)) {
                        // convert user object to json string and return it
                        String jsonString = article.convert_to_Json().toString();
                        publishToRMQ(jsonString, queueElk);

                        log.info("Parsing new html" + urls);
                        for (String url_ : urls) {
                            log.info("Add to queueDownload new url: " + url_);
                            publishToRMQ(url_, queueDownload);
                        }

                    } else {
                        log.info("Document already exist in elk wirh sha256: " + article.sha256);
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

                timeTag = parsedDoc.getElementsByClass("meta__text").attr("datetime");
                //2023-06-13T17:21:28+0300
                formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sszzz", Locale.ENGLISH);
                date = formatter.parse(timeTag);
                log.info("dates" + date);
            }catch (Exception e){
                sleep(1000);
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
                author = parsedDoc.getElementsByClass("article__author-image").attr("alt");
            }catch(Exception e){
                author = "";
            }

            try {
                contents = parsedDoc.getElementsByClass("article__body").select("p");
                content = "";
                for (Element element : contents) {
                    content += element.text() + "\n";
                }
                //log.info(content);
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
            Elements aTag = parsedDoc.getElementsByClass("news-listing__item"). select("a"); //.getElementsByClass("w_col2"). select("a");
            for (Element element : aTag) {
                try {
                    String link = element.attr("href");
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
