package mgtu.SiteFetcher;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static Thread taskProducer;
    private static Thread taskConsumer;
    private static String site = "https://mcpromo.ru/e";//https://mytyshi.ru/";

    public static void main(String[] args) {
        BasicConfigurator.configure();
        try {
            RabbitMqCreds rabbitCreds = new RabbitMqCreds("rabbitmq",
                    "rabbitmq",
                    "/",
                    "127.0.0.1",
                    5672);
            taskProducer = new Thread(new TaskProducer(rabbitCreds));
            taskProducer.start();

            taskConsumer = new Thread(new TaskConsumer(rabbitCreds));
            taskConsumer.start();

        } catch (Exception e) {
            log.error(e);
        }
    }

}
