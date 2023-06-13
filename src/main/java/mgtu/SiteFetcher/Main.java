package mgtu.SiteFetcher;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.http.HttpHost;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Main {
    private static Logger log = LogManager.getLogger();
    //private static Thread taskProducer;
    //private static Thread taskConsumer;
    //private static Thread taskES;


    public static void main(String[] args) {
        BasicConfigurator.configure();
        try {
            RabbitMqCreds rabbitCreds = new RabbitMqCreds("rabbitmq",
                    "rabbitmq",
                    "/",
                    "127.0.0.1",
                    5672);
            for (int i = 0; i < 1; i++) {
                Thread taskES = new Thread(new ElasticSearch(rabbitCreds));
                taskES.start();
            }

            for (int i = 0; i < 2; i++) {
                Thread taskProducer = new Thread(new TaskProducer(rabbitCreds));
                taskProducer.start();
            }
            for (int i = 0; i < 2; i++) {
                Thread taskConsumer = new Thread(new TaskConsumer(rabbitCreds));
                taskConsumer.start();
            }



        } catch (Exception e) {
            log.error(e);
        }
    }

}
