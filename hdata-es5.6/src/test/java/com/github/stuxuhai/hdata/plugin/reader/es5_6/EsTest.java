package com.github.stuxuhai.hdata.plugin.reader.es5_6;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class EsTest {

    private static TransportClient client;


    private static String getScrollId(String index, String indexType, TimeValue timeValue) {
        // 搜索条件
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setIndices(index);
        searchRequestBuilder.setTypes(indexType);
        searchRequestBuilder.setScroll(timeValue);

        QueryBuilder queryBuilder = new TermQueryBuilder("subdomain", "mil.news.sina.com.cn");
        searchRequestBuilder.setQuery(queryBuilder);

        // 执行
        SearchResponse searchResponse = searchRequestBuilder.get();
        String scrollId = searchResponse.getScrollId();
        long totalHits = searchResponse.getHits().getTotalHits();

        System.out.println("scrollId = " + scrollId);
        System.out.println("totalHits = " + totalHits);

        return scrollId;
    }

    public static void main(String[] args) throws Exception {
//        Map map = new HashMap();
//        map.put("1", 12);
//        System.out.println(map.get("3"));

//        String str = "user:123345";
//        String[] arr2 = str.split(":");
//        System.out.println(arr2[0]);
//        System.out.println(arr2[1]);

        Settings settings = Settings.builder()
                .put("cluster.name", "es-188").build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.188"), 9300));

        String index = "index_1000";
        String indexType = "kxtbdoc";

        TimeValue timeValue = new TimeValue(30000);

        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse response;
        String scrollId = null;
        long total = 0l;

        while (true) {
            if (scrollId == null) {
                SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
                searchRequestBuilder.setIndices(index);
                searchRequestBuilder.setTypes(indexType);
                searchRequestBuilder.setScroll(timeValue);
                QueryBuilder queryBuilder = new TermQueryBuilder("subdomain", "mil.news.sina.com.cn");
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
                String source = searchHit.getSource().toString();
                System.out.println(source);
            } // for
            scrollId = response.getScrollId();
            total += searchHits.length;
        }

        System.out.println("total = " + total);

//            SearchResponse response = client
//                    .prepareSearch(index)
//                    .setTypes(indexType)
//                    .setSearchType(SearchType.DEFAULT)
//                    .setFrom(start)
//                    .setSize(step)
//                    .get();
//            SearchHits searchHits = response.getHits();
//            SearchHit[] arr = searchHits.getHits();
//            if (arr.length == 0) break;
//            for (SearchHit hit : arr) {
//                Map<String, Object> sources = hit.getSource();
//                System.out.println(sources);
//            }
//            start = start + step;
//        }

//        String filter = args[0];
//
//        System.out.println("filter : " + filter);
//
//        QueryBuilder queryBuilder = new TermQueryBuilder("user", filter);

//        QueryBuilder queryBuilder = new QueryStringQueryBuilder(filter);

//        SearchResponse response = client.prepareSearch(index).setTypes(indexType).addAggregation(AggregationBuilders.max("max_post_date").field("postDate")).get();
//
//        InternalMax aggregation = response.getAggregations().get("max_post_date");
//
//        System.out.println(aggregation.getValue());

//        SearchResponse response = client
//                .prepareSearch(index)
//                .setTypes(indexType)
//                .setFrom(start)
//                .setSize(step)
//                .get();
//        SearchHits searchHits = response.getHits();
//        SearchHit[] arr = searchHits.getHits();
//        for (SearchHit hit : arr) {
//            Map<String, Object> sources = hit.getSource();
//            System.out.println(sources);
//        }

//        for (int i = 0; i < 234; i += 1) {
//            IndexResponse response = client.prepareIndex(index, indexType, String.valueOf(i))
//                    .setSource(jsonBuilder()
//                            .startObject()
//                            .field("user", "joey#" + i)
//                            .field("age", i)
//                            .field("postDate", new Date())
//                            .field("message", "trying out Elasticsearch#" + i)
//                            .endObject()
//                    )
//                    .get();
//            System.out.println("doc #" + response.getId() + " created");
//        }

//        IndexResponse response = client.prepareIndex("twitter", "tweet", "66")
//                .setSource(jsonBuilder()
//                        .startObject()
//                        .field("user", "kimchy")
//                        .field("postDate", new Date())
//                        .field("message", "trying out Elasticsearch")
//                        .endObject()
//                )
//                .get();
//        System.out.println(response.getId());
    }
}
