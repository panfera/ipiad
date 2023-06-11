package mgtu.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.elasticsearch.common.transport.I
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ElasticConnector {
    private static Logger log = LogManager.getLogger();
    private Config config;
    private PreBuiltTransportClient client;
    private String indexName = "test-index";
    private String docType = "test";

    public void initialize(Config config) {
        this.config = config;
        try {
            client = createClient();
            indexName = config.getString("indexName");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private PreBuiltTransportClient createClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", config.getString("cluster"))
                //.put("client.transport.sniff", false)
                .build();
        PreBuiltTransportClient cli = new PreBuiltTransportClient(settings);
        cli.addTransportAddress(
                new TransportAddress(InetAddress.getByName(config.getString("host")),
                        config.getInt("port")));
        //if (config.getString("password"))
        //    token = basicAuthHeaderValue(config.getString("user"), new SecureString(config.getString("password").toCharArray()));
        return cli;
    }

    IndexResponse saveSomeData(Map<String, Object> document) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(document);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(json.getBytes());
            String idHash = Hex.encodeHexString(md.digest());
            IndexRequestBuilder ind = client.prepareIndex(indexName, docType, idHash).setSource(document);
            IndexResponse res = ind.execute().actionGet();
            log.debug(res);
            return res;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    BulkResponse deleteBulkData(List<String> ids) {
        BulkRequestBuilder bulk = client.prepareBulk();
        int requests = 0;
        for (String id : ids) {
            bulk.add(new DeleteRequest().id(id).index(indexName)/*.type(docType)*/);
        }
        if (ids.size() > 0)
            return bulk.execute().actionGet();
        return null;
    }
    BulkResponse saveBulkData(Map<String, Map<String, Object>> documents) {
        BulkRequestBuilder bulk = client.prepareBulk();
        int requests = 0;
        try {
            for (Map.Entry<String, Map<String, Object>> entry : documents.entrySet()) {
                String docID = entry.getKey();
                Map<String, Object> content = entry.getValue();
                if (content == null || content.size() == 0) {
                    // delete
                    //if (log.isTraceEnabled())
                    //    log.trace("Deleting entire document {}", docID);
                    bulk.add(new DeleteRequest(indexName, docID));
                    requests++;
                } else {
                    // Add
                    //if (log.isTraceEnabled())
                    //    log.trace("Adding entire document {}", docID);
                    bulk.add(new IndexRequest(indexName, docID).source(content));
                    requests++;
                }
            }
            if (requests > 0)
                return bulk.execute().actionGet();
        } catch (Exception e) {
            //throw e;
        }
        return null;
    }
    SearchHits getSomeData(String value) {
        QueryBuilder query = QueryBuilders.termQuery("nickname", value);
        SearchResponse response = client.prepareSearch(indexName).setQuery(query).get();
        log.debug(response.getHits().getTotalHits());
        return response.getHits();
    }
    SearchHits getSomeDataList(String value) {
        QueryBuilder query = QueryBuilders.termQuery("nickname", value);
        SearchResponse response = client.prepareSearch(indexName).setQuery(query).get();
        Iterator<SearchHit> sHits = response.getHits().iterator();
        List<String> results = new ArrayList<String>(20); //some hack! initial size of array!
        while (sHits.hasNext()) {
            results.add(sHits.next().getSourceAsString());
        }
        log.debug(response.getHits().getTotalHits());
        return response.getHits();
    }
}
