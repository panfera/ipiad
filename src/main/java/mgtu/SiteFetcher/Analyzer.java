package mgtu.SiteFetcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.python.util.PythonInterpreter;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleScriptContext;
import java.io.*;
import java.util.List;
import java.util.Map;

import static javatests.TestSupport.assertEquals;
import static mgtu.SiteFetcher.ElasticSearch.get_text;

public class Analyzer {
    public static Logger log = LogManager.getLogger();

    public void run_py_script(String author) throws IOException {
        SearchHits hits = get_text(author);

        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            String content = (String) source.get("content");
            log.info("get text");

            ProcessBuilder processBuilder = new ProcessBuilder("python", "analyzer.py", content);
            processBuilder.redirectErrorStream(true);
            Process process = null;

            try {
                process = processBuilder.start();
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                int s = br.read();
                while (s != -1) {
                    System.out.print((char) s);
                    s = br.read();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
