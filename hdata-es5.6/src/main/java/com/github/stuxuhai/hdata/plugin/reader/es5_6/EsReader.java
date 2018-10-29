package com.github.stuxuhai.hdata.plugin.reader.es5_6;

import com.github.stuxuhai.hdata.api.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsReader extends Reader {

    private static final Logger LOG = LogManager.getLogger(EsReader.class);

    private TransportClient client;

    private String index;

    private String indexType;

    private String filter;

    private String[] columns;

    private String cursorColumn;

    private String cursorValue;

    private QueryBuilder queryBuilder;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        index = readerConfig.getString(EsReaderProperties.INDEX);
        indexType = readerConfig.getString(EsReaderProperties.INDEX_TYPE);
        columns = StringUtils.splitPreserveAllTokens(readerConfig.getString(EsReaderProperties.COLUMNS), ",");
        queryBuilder = (QueryBuilder) readerConfig.get(EsReaderProperties.QUERY);
        client = getClient(readerConfig);
        LOG.info("ES client init done.");
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        TimeValue timeValue = new TimeValue(30000);

        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse response;
        String scrollId = null;

        while (true) {
            if (scrollId == null) {
                SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
                searchRequestBuilder.setScroll(timeValue);
                searchRequestBuilder.setIndices(index);
                searchRequestBuilder.setTypes(indexType);
                searchRequestBuilder.setQuery(queryBuilder);
                response = searchRequestBuilder.get();
            } else {
                searchScrollRequestBuilder = client.prepareSearchScroll(scrollId);
                searchScrollRequestBuilder.setScroll(timeValue);
                response = searchScrollRequestBuilder.get();
            }
            if (response.getHits().getHits().length == 0) {
                break;
            } // if
            SearchHit[] searchHits = response.getHits().getHits();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> sources = searchHit.getSource();
                Record record = new DefaultRecord(columns.length);
                for (String c : columns) {
                    Object value = sources.get(c);
                    if (value == null) {
                        value = "";
                    }
                    record.add(value);
                }
                recordCollector.send(record);
            } // for
            scrollId = response.getScrollId();
        }
    }

    private TransportClient getClient(PluginConfig readerConfig) {
        String clusterName = readerConfig.getString(EsReaderProperties.CLUSTER_NAME);
        String ipAddresses = readerConfig.getString(EsReaderProperties.IP_ADDRESSES);
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName).build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            for (String ipAndPort : ipAddresses.split(",")) {
                String[] ipPort = ipAndPort.split(":");
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ipPort[0]), Integer.parseInt(ipPort[1])));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    @Override
    public Splitter newSplitter() {
        return new Splitter() {
            @Override
            public List<PluginConfig> split(JobConfig jobConfig) {
                PluginConfig readerConfig = jobConfig.getReaderConfig();
                filter = readerConfig.getString(EsReaderProperties.FILTER);
                cursorColumn = readerConfig.getString(EsReaderProperties.CURSOR_COLUMN);
                cursorValue = readerConfig.getString(EsReaderProperties.CURSOR_VALUE);
                index = readerConfig.getString(EsReaderProperties.INDEX);
                indexType = readerConfig.getString(EsReaderProperties.INDEX_TYPE);
                List<PluginConfig> ret = new ArrayList();
                List<QueryBuilder> queryBuilders = new ArrayList();
                if (StringUtils.isNotBlank(filter)) {
                    String[] arr = filter.split(";");
                    for (String str : arr) {
                        String[] arr2 = str.split("=");
                        queryBuilders.add(QueryBuilders.termQuery(arr2[0], arr2[1]));
                    }
                }
                if (StringUtils.isNotBlank(cursorColumn)) {
                    client = getClient(readerConfig);
                    SearchResponse response = client.prepareSearch(index).setTypes(indexType).addAggregation(AggregationBuilders.max("max_cursor_value").field(cursorColumn)).get();
                    InternalMax internalMax = response.getAggregations().get("max_cursor_value");
                    if (internalMax != null) {
                        double max = internalMax.getValue();
                        jobConfig.setString("CursorValue", String.valueOf(max));
                    }
                    if (StringUtils.isNotBlank(cursorValue)) {
                        queryBuilders.add(QueryBuilders.rangeQuery(cursorColumn).gt(cursorValue));
                    }
                }
                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                for (QueryBuilder queryBuilder : queryBuilders) {
                    boolQuery.must(queryBuilder);
                }

                readerConfig.put(EsReaderProperties.QUERY, boolQuery);
                ret.add(readerConfig);
                return ret;
            }
        };
    }


}
