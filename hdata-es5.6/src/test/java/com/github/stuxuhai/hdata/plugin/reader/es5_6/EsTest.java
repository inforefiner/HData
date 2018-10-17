package com.github.stuxuhai.hdata.plugin.reader.es5_6;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class EsTest {

    private static TransportClient client;

    public static void main(String[] args) throws Exception {
//        Map map = new HashMap();
//        map.put("1", 12);
//        System.out.println(map.get("3"));

        String str = "user:123345";
        String[] arr2 = str.split(":");
        System.out.println(arr2[0]);
        System.out.println(arr2[1]);

        Settings settings = Settings.builder()
                .put("cluster.name", "es-188").build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.188"), 9300));

        String index = "test_index";
        String indexType = "test_index_type";


        int start = 0;
        int step = 100;

        String filter = args[0];

        System.out.println("filter : " + filter);

        QueryBuilder queryBuilder = new TermQueryBuilder("user", filter);

//        QueryBuilder queryBuilder = new QueryStringQueryBuilder(filter);

//        SearchResponse response = client.prepareSearch(index).setTypes(indexType).addAggregation(AggregationBuilders.max("max_post_date").field("postDate")).get();
//
//        InternalMax aggregation = response.getAggregations().get("max_post_date");
//
//        System.out.println(aggregation.getValue());

//        SearchResponse response = client
//                .prepareSearch(index)
//                .setTypes(indexType)
//                .setQuery(queryBuilder)
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
