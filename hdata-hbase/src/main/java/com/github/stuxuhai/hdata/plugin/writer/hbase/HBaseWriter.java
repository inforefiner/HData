package com.github.stuxuhai.hdata.plugin.writer.hbase;

import com.alibaba.fastjson.JSON;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Preconditions;
import com.merce.woven.data.rpc.ObjectService;
import org.apache.commons.lang.StringUtils;
import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
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
        conf.set("hbase.client.keyvalue.maxsize", writerConfig.getString(HBaseWriterProperties.HBASE_VALUE_SIZE, "104857600"));
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
					RegistryConfig registry = new RegistryConfig();
					String protocol = writerConfig.getString("protocol", "zookeeper");
					registry.setProtocol(protocol);
					registry.setClient("curatorx");

					registry.setAddress(writerConfig.getString("address"));
					registry.setUsername(writerConfig.getString("username"));
					registry.setPassword(writerConfig.getString("password"));
					try {
						File file = File.createTempFile("dubbo-" + System.currentTimeMillis(), ".cache");
						file.deleteOnExit();
						registry.setFile(file.getPath());
						logger.info("cached registry file in " + file.getPath());
					} catch (IOException e) {
						e.printStackTrace();
					}

					ReferenceConfig<ObjectService> reference = new ReferenceConfig<>();
					reference.setApplication(application);
					reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
					reference.setInterface(ObjectService.class);
					reference.setTimeout(60 * 1000);

					ConsumerConfig consumerConfig = new ConsumerConfig();
					consumerConfig.setSticky(true);
					consumerConfig.setTimeout(60 * 1000);
					reference.setConsumer(consumerConfig);

					try {
						objectService = reference.get();
					} catch (Throwable e) {
						logger.error("can't connect registry rpc-object-service", e);
					}
				}
			}
		}
		return objectService;
    }
}
