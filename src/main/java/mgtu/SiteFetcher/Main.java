package mgtu.SiteFetcher;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.net.URI;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static Thread taskController;
    private static String site = "https://mcpromo.ru/e";//https://mytyshi.ru/";

    static public void ParseNews(Document doc) {
        try {
            Elements news = doc.getElementsByClass("home--box-c card-layout is-col3").select("div > div > div > h4.card--title > a");
            for (Element element : news) {
                try {
                    String link = element.attr("href");
                    log.info(element.text());
                    //link = "https://mytyshi.ru/article/molodym-mytischintsam-vruchili-pasporta-grazhdan-rossijskoj-federatsii-313452";
                    //String text = taskController.GetPage(link);
                    //log.info(text);
                } catch (Exception e) {
                    log.error(e);
                }
            }
        } catch (Exception ex){
            log.error(ex);
        }
        return;
    }
    public static void main(String[] args){
       // taskController = new TaskController(site);
        //Document doc = taskController.getUrl(site);
        BasicConfigurator.configure();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setVirtualHost("/");
            factory.setPort(5672);
            factory.setHost("127.0.0.1");
            //factory.setUri(new URI("http://rabbitmq:rabbitmq@localhost:5672"));

            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();

            taskController = new Thread(new TaskController(channel, site));
            taskController.start();

        } catch (Exception e){
            log.error(e);
            return;
        }
        String title;
        /*
        if (doc != null){
            title = doc.title();
            log.info(title);
            ParseNews(doc);
        }*/
        return;
    }
}
