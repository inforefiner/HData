package com.github.stuxuhai.hdata.plugin.writer.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Preconditions;
import com.merce.woven.data.rpc.ObjectService;
import org.apache.commons.lang.StringUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HBaseWriter extends Writer {

    private final Logger logger = LoggerFactory.getLogger(HBaseWriter.class);

    private static AtomicInteger ChannelGenerator = new AtomicInteger();

    private Table table;

    private int batchSize;

    private final List<Put> putList = new ArrayList<>();

    private String[] columns;

    private String[] rowkeyCol;

    private String namespace;

    private String tableName;

    private String tenantId;

    private String taskId;

    private String serviceType;

    private String channelId;

    private AtomicLong in = new AtomicLong(0L);

    private AtomicLong out = new AtomicLong(0L);

    private static ObjectService objectService;

    private JobContext jobContext;

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        Configuration conf = HBaseConfiguration.create();
        if (writerConfig.containsKey(HBaseWriterProperties.ZOOKEEPER_ZNODE_PARENT)) {
            conf.set(HBaseWriterProperties.ZOOKEEPER_ZNODE_PARENT,
                    writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_ZNODE_PARENT));
        }

        Preconditions.checkNotNull(writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_QUORUM),
                "HBase writer required property: zookeeper.quorum");

        conf.set("hbase.zookeeper.quorum", writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_QUORUM));
        conf.set("hbase.zookeeper.property.clientPort",
                writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
        batchSize = writerConfig.getInt(HBaseWriterProperties.BATCH_INSERT_SIZE, 5);

        Preconditions.checkNotNull(writerConfig.getString("fields"),
                "HBase writer required property: fields");
        columns = writerConfig.getString("fields").split(",");

        Preconditions.checkNotNull(writerConfig.getString("rowKey"),
                "HBase writer required property: rowKey");
        rowkeyCol = writerConfig.getString("rowKey").split(",");

        namespace = writerConfig.getString("namespace");

        tableName = writerConfig.getString(HBaseWriterProperties.TABLE);

        tenantId = writerConfig.getString("tenantId");

        taskId = writerConfig.getString("taskId");

        serviceType = writerConfig.getString("serviceType");

        channelId = "" + ChannelGenerator.getAndIncrement();

        this.jobContext = context;

        String cursorValue = context.getJobConfig().getString("CursorValue");
        if (cursorValue != null) {
            writerConfig.setString("CursorValue", cursorValue);
        }

        objectService = connectWriterServer(writerConfig);

        objectService.prepare(tenantId, taskId, writerConfig);

        try {
            Preconditions.checkNotNull(tableName, "HBase writer required property: table");
            Connection conn = ConnectionFactory.createConnection(conf);
            createTable(conn, namespace, tableName, new String[]{"kf", "vf"});
            table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        } catch (IOException e) {
            objectService.onError(tenantId, taskId, channelId, new RuntimeException("hbase conn error"));
            throw new HDataException(e);
        }

    }

    @Override
    public void execute(Record record) {
        StringBuilder rowkey = new StringBuilder();
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < rowkeyCol.length; i++) {
            rowkey.append(record.get(i));
            map.put(rowkeyCol[i], record.get(i));
        }
        Put put = new Put(Bytes.toBytes(SHA256Util.getSHA256StrJava(rowkey.toString())));
        for (int i = rowkeyCol.length; i < rowkeyCol.length + columns.length; i++) {
            Object o = record.get(i);
            if (o instanceof Clob) {
                if (o instanceof NClob) {
                    NClob nClob = (NClob) o;
                    try {
                        o = nClob.getSubString(1, (int) nClob.length());
                    } catch (Throwable e) {
                        o = "";
                    }
                } else {
                    Clob clob = (Clob) o;
                    try {
                        o = clob.getSubString(1, (int) clob.length());
                    } catch (Throwable e) {
                        o = "";
                    }
                }
            }
            if (o instanceof Blob) {
                Blob blob = (Blob) o;
                try {
                    o = new String(blob.getBytes(1, (int) blob.length()), "UTF8");
                } catch (Throwable e) {
                    o = "";
                }
            }

            put.addColumn(Bytes.toBytes("vf"), Bytes.toBytes(columns[i - rowkeyCol.length]), Bytes.toBytes(o.toString()));
        }
        put.addColumn(Bytes.toBytes("kf"), Bytes.toBytes("kf"), Bytes.toBytes(JSON.toJSON(map).toString()));
        in.incrementAndGet();

//		Object rowkeyValue = record.get(rowkeyIndex);
//		Put put = new Put(Bytes.toBytes(rowkeyValue == null ? "NULL" : rowkeyValue.toString()));
//		for (int i = 0, len = record.size(); i < len; i++) {
//			if (i != rowkeyIndex) {
//				String[] tokens = columns[i].split(":");
//				put.addColumn(Bytes.toBytes(tokens[0]), Bytes.toBytes(tokens[1]),
//						record.get(i) == null ? null : Bytes.toBytes(record.get(i).toString()));
//			}
//		}

        putList.add(put);
        if (putList.size() == batchSize) {
            try {
                table.put(putList);
            } catch (IOException e) {
                objectService.onError(tenantId, taskId, channelId, new RuntimeException("write hbase has error !"));
                throw new HDataException(e);
            }
            out.addAndGet(putList.size());
            putList.clear();
            objectService.onUpdate(tenantId, taskId, channelId, in.get(), out.get());
        }
    }

    @Override
    public void close() {
        if (table != null) {
            try {
                if (putList.size() > 0) {
                    table.put(putList);
                }
                table.close();
            } catch (IOException e) {
                objectService.onError(tenantId, taskId, channelId, new RuntimeException("write hbase has error !"));
                throw new HDataException(e);
            }
            out.addAndGet(putList.size());
            putList.clear();
        }
        if (jobContext.isReaderError() || jobContext.isWriterError()) {
            objectService.onError(tenantId, taskId, channelId, new RuntimeException("reader or writer has error !"));
        } else {
            objectService.onFinish(tenantId, taskId, channelId, in.get(), out.get());
        }
    }

    public static void createTable(Connection connection, String namespace, String tableName, String[] cols) throws IOException {
        String join = namespace + ":" + tableName;
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(join);
        if (StringUtils.isNotBlank(namespace)) {
            NamespaceDescriptor nsd = NamespaceDescriptor.create(namespace).build();
            try {
                admin.createNamespace(nsd);
            } catch (Exception e) {
            }
        }
        if (!admin.tableExists(table)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    public ObjectService connectWriterServer(com.github.stuxuhai.hdata.api.Configuration writerConfig) {
        if (objectService == null) {
            synchronized (ObjectService.class) {
                if (objectService == null) {
                    ApplicationConfig application = new ApplicationConfig();
                    application.setName("hdata-dubbo-object-writer");
                    application.setQosEnable(false);
                    RegistryConfig registry = new RegistryConfig();
                    String protocol = writerConfig.getString("protocol", "consul");
                    registry.setProtocol(protocol);
                    registry.setAddress(writerConfig.getString("address"));

                    ReferenceConfig<ObjectService> reference = new ReferenceConfig<>();
                    reference.setApplication(application);
                    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
                    reference.setInterface(ObjectService.class);
                    reference.setTimeout(60 * 1000);

                    String clusterId = writerConfig.getString("cluster.id");
                    String url = getUrl(writerConfig.getString("address"), clusterId);
                    logger.info("url: {}", url);
                    reference.setUrl(url);

                    ConsumerConfig consumerConfig = new ConsumerConfig();
                    consumerConfig.setSticky(true);
                    consumerConfig.setTimeout(60 * 1000);
                    reference.setConsumer(consumerConfig);

                    try {
                        objectService = reference.get();
                    } catch (Exception e) {
                        logger.error("can't connect registry rpc-file-service", e);
                    }
                }
            }
        }
        return objectService;
    }

    private String getUrl(String address, String clusterId) {
        logger.info("address: {}, cluster id: {}", address, clusterId);
        String serviceName = "providers:"+ObjectService.class.getName()+"::";
        try {
            NacosNamingService namingService = new NacosNamingService(address) ;
            List<Instance> list = namingService.getAllInstances(serviceName);

            if(list != null && list.size() > 0){
                if(StringUtils.isEmpty(clusterId)){
                    Instance instance = list.get(0);
                    return instance.getIp()+ ":" + instance.getPort();
                }

                for(Instance instance : list){
                    if(instance.getMetadata().containsKey("clusterId")){
                        if(clusterId.equals(instance.getMetadata().get("clusterId"))){
                            return instance.getIp()+ ":" + instance.getPort();
                        }
                    }
                }
            }else{
                logger.error("get available service for " + serviceName + " is null");
                throw new RuntimeException("get available service for " + serviceName + " is null");
            }

            logger.error("can't get available service for " + serviceName);
            throw new RuntimeException("can't get available service for " + serviceName);
        }catch (Exception e){
            logger.error("get available service exception for " + serviceName, e);
            throw new RuntimeException("get available service exception for " + serviceName);
        }
    }
}
