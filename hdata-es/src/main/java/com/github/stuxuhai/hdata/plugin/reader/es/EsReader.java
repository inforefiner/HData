package com.github.stuxuhai.hdata.plugin.reader.es;

import com.github.stuxuhai.hdata.api.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class EsReader extends Reader {

    private static final Logger LOG = LogManager.getLogger(EsReader.class);

    private TransportClient client;

    private String index;

    private String indexType;

    private String filter;

    private String[] columns;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {

        String clusterName =  readerConfig.getString(EsReaderProperties.CLUSTER_NAME);
        String ipAddresses =  readerConfig.getString(EsReaderProperties.IP_ADDRESSES);

        index = readerConfig.getString(EsReaderProperties.INDEX);
        indexType = readerConfig.getString(EsReaderProperties.INDEX_TYPE);
        columns = readerConfig.getString(EsReaderProperties.COLUMNS).split(",");
        filter = readerConfig.getString(EsReaderProperties.FILTER, "{}");

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();

        client = new TransportClient(settings);
        try {
            for (String ipAndPort : ipAddresses.split(",")) {
                String[] ipPort = ipAndPort.split(":");
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ipPort[0]), Integer.parseInt(ipPort[1])));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        LOG.info("ES client init done. ipAddresses = {}, clusterName = {}", ipAddresses, clusterName);
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        int start = 0;
        int step = 1000;
        while(true){
            SearchResponse response = client
                    .prepareSearch(index)
                    .setTypes(indexType)
                    .setPostFilter(filter)
                    .setFrom(start)
                    .setSize(step)
                    .get();
            SearchHits searchHits = response.getHits();
            SearchHit[] arr = searchHits.getHits();
            if (arr.length == 0) break;
            for (SearchHit hit: arr){
                Map<String, Object> sources = hit.getSource();
                Record record = new DefaultRecord(columns.length);
                for(String c: columns){
                    record.add(sources.get(c));
                }
                recordCollector.send(record);
            }
            start = start + step;
        }
    }

    @Override
    public Splitter newSplitter() {
        return null;
    }


}
