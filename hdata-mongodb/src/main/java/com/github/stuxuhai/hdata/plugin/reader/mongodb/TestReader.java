package com.github.stuxuhai.hdata.plugin.reader.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class TestReader {


    public static void main(String[] args) {

        int a = 8;
        int b = 3;
        int d = (int) Math.ceil((double) a / (double) b);
        System.out.println(d);


        MongoClient client = MongoClients.create("mongodb://zkhh:zkhh123@182.92.162.12:27017/?authSource=wx");


//        MongoClient mongoClient = MongoClients.create(
//                MongoClientSettings.builder()
//                        .applyToClusterSettings(builder ->
//                                builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
//                        .credential(credential)
//                        .build());
//        ServerAddress serverAddress = new ServerAddress("182.92.162.12", 27017);
//        MongoCredential.createPlainCredential()
//        MongoClient client = new MongoClient(serverAddress, Arrays.asList(MongoCredential.createCredential("zkhh", "wx", "zkhh123".toCharArray())), MongoClientOptions.builder().build());
        MongoDatabase database = client.getDatabase("wx");
        MongoCollection c = database.getCollection("signs");

        String cursorValue = "";

        Document max = (Document) c.find().sort(new BasicDBObject("_id", -1)).iterator().next();
        String maxId = max.getObjectId("_id").toHexString();
        List<Bson> query = new ArrayList<>();
        if (StringUtils.isNotBlank(cursorValue)) {
            query.add(Filters.gt("_id", new ObjectId(cursorValue)));
        }
        query.add(Filters.lte("_id", new ObjectId(maxId)));
        Long count = c.countDocuments(Filters.and(query));
        System.out.println("count = " + count);
        int MIN_BATCH_SIZE = 5000;
        int parallelism = 2;
        int batch = MIN_BATCH_SIZE;
        int pCount = count.intValue() / parallelism;
        if (batch < pCount) {
            batch = pCount;
        }
        for (int i = 0; i < parallelism; i++) {
            int skip = i * batch;
            if (skip > count) {
                break;
            }
            System.out.println("skip = " + skip + ", batch = " + batch);
//            PluginConfig otherReaderConfig = (PluginConfig) readerConfig.clone();
//            FindIterable<Document> iterable = c.find(Filters.and(query)).skip(skip).limit(batch);
//            otherReaderConfig.put(MongoDBReaderProperties.ITERATOR, iterable);
//            ret.add(otherReaderConfig);
        }

//        collection.

//        Bson sort = new BasicDBObject("_id", -1);
//        FindIterable<Document> iterable = collection.find().sort(sort).max(new BasicDBObject("_id", "5ab483cdcff8d680186525d6")).limit(10);
//        FindIterable<Document> iterable = collection.find(Filters.gt("_id", new ObjectId("58733910cb2018ec1e1a0c9f"))).sort(query);
//        MongoCursor<Document> cursor = iterable.iterator();
//        while (cursor.hasNext()) {
//            Document document = cursor.next();
//            ObjectId id = (ObjectId)document.get("_id");
//            System.out.println(id.toHexString());//5b76632e3cfa776c1ae3fd19
//            System.out.println(Long.valueOf(id, 16));
//        iterable.
//        while(iterable.){
//
//        }
//        long l = collection.countDocuments();
//        System.out.println(l);
    }
}
