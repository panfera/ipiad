package mgtu.SiteFetcher;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

public class Article {
    String title;
    String author;
    String url;
    Date date;
    String content;

    String sha256;
    private static Logger log = LogManager.getLogger();

    public Article(String title, String author, String url, Date date, String content) {
        this.title = title;
        this.author = author;
        this.url = url;
        this.date = date;
        this.content = content;
        try {
            this.sha256 = getSHA256(url + date);
        }catch (Exception e){
            this.sha256 = "";
            log.error(e);
        }

    }

    private static String getSHA256(String input) throws NoSuchAlgorithmException
    {
        // Static getInstance method is called with hashing SHA
        MessageDigest md = MessageDigest.getInstance("SHA-256");

        // digest() method called
        // to calculate message digest of an input
        // and return array of byte
        return toHexString(md.digest(input.getBytes(StandardCharsets.UTF_8)));
    }

    private static String toHexString(byte[] hash)
    {
        // Convert byte array into signum representation
        BigInteger number = new BigInteger(1, hash);

        // Convert message digest into hex value
        StringBuilder hexString = new StringBuilder(number.toString(16));

        // Pad with leading zeros
        while (hexString.length() < 64)
        {
            hexString.insert(0, '0');
        }

        return hexString.toString();
    }

    public Article(String str) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
        JSONObject json = new JSONObject(str);
        this.title = json.getString("title");
        this.author = json.getString("author");
        this.url = json.getString("url");
        this.date = formatter.parse(json.getString("date"));;
        this.content = json.getString("content");
        this.sha256 = json.getString("sha256");
    }
    public JSONObject convert_to_Json() {
        JSONObject json = new JSONObject();
        json.put("title", title);
        json.put("author", author);
        json.put("url", url);
        json.put("date", date);
        json.put("content", content);
        json.put("sha256", sha256);
        log.info("json: " + json);
        return json;
    }
}
