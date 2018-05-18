package com.github.stuxuhai.hdata.plugin.reader.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.plugin.reader.http.client.GetClient;
import com.github.stuxuhai.hdata.plugin.reader.http.client.JsonBuilder;
import com.github.stuxuhai.hdata.plugin.reader.http.client.PostClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;

public class HttpReader extends Reader {

    private static final Logger LOG = LogManager.getLogger(HttpReader.class);

    private String urlstr = null;
    private String method = null;
    private String encoding = null;
    private String parameters = null;
    private String[] fields = null;
    private String rootPath = null;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        urlstr = readerConfig.getString(HttpReaderProperties.URL);
        method = readerConfig.getString(HttpReaderProperties.METHOD, "GET");
        parameters = readerConfig.getString(HttpReaderProperties.PARAMETERS);
        encoding = readerConfig.getString(HttpReaderProperties.ENCODING, "UTF-8");
        fields = readerConfig.getString(HttpReaderProperties.FIELDS).split(",");
        rootPath = readerConfig.getString(HttpReaderProperties.ROOT_PATH);
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        Object resp = null;
        if ("POST".equalsIgnoreCase(method)) {
            resp = new PostClient(urlstr).doPost(parameters).getResponse();
        } else {
            if (parameters != null) {
                urlstr = urlstr + "?" + parameters;
            }
            resp = new GetClient(urlstr).doGet().getResponse();
        }

        JsonNode jsonNode = JsonBuilder.getInstance().fromJson(resp.toString(), JsonNode.class);

        if (rootPath != null && rootPath.length() > 0) {
            for (String path : rootPath.split("/")) {
                jsonNode = jsonNode.findPath(path);
            }
        }

        if (jsonNode.isArray()) {
            Iterator<JsonNode> iterator = jsonNode.iterator();
            while (iterator.hasNext()) {
                JsonNode node = iterator.next();
                Record record = new DefaultRecord(fields.length);
                for (String field : fields) {
                    if(field.isEmpty()){
                        record.add("");
                    }else {
                        record.add(node.get(field).textValue());
                    }
                }
                recordCollector.send(record);
            }
        }
    }

    @Override
    public Splitter newSplitter() {
        return new HttpSplitter();
    }


    public static void main(String[] args) {
        Object resp = new GetClient("http://top.baidu.com/news/pagination?pageno=1").doGet().getResponse();
        JsonNode jsonNode = JsonBuilder.getInstance().fromJson(resp.toString(), JsonNode.class);

        System.out.println(jsonNode.isArray());
    }
}
