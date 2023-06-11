package mgtu.elastic;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        // write your code here
        Config conf = ConfigFactory.load();
        ElasticConnector esCon = new ElasticConnector();
        esCon.initialize(conf.getConfig("es"));
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("nickname", "vasya");
        esCon.saveSomeData(data);
        esCon.getSomeData("vasya");
    }
}