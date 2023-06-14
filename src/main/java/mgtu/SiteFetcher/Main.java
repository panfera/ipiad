package mgtu.SiteFetcher;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static Logger log = LogManager.getLogger();
    //private static Thread taskProducer;
    //private static Thread taskConsumer;
    //private static Thread taskES;
    private static int count_producer = 2;
    private static int count_consumer = 2;
    private static int count_es = 1;
    protected static String author = "Эмма Грибова";

    public static void main(String[] args) {
        BasicConfigurator.configure();
        try {
            RabbitMqCreds rabbitCreds = new RabbitMqCreds("rabbitmq",
                    "rabbitmq",
                    "/",
                    "127.0.0.1",
                    5672);
            for (int i = 0; i < count_es; i++) {
                Thread taskES = new Thread(new ElasticSearch(rabbitCreds));
                taskES.start();
            }

            for (int i = 0; i < count_producer; i++) {
                Thread taskProducer = new Thread(new TaskProducer(rabbitCreds));
                taskProducer.start();
            }
            for (int i = 0; i < count_consumer; i++) {
                Thread taskConsumer = new Thread(new TaskConsumer(rabbitCreds));
                taskConsumer.start();
            }

            Analyzer analyzer = new Analyzer();
            analyzer.run_py_script(author);

        } catch (Exception e) {
            log.error(e);
        }
    }

}
