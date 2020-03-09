package com.github.stuxuhai.hdata.plugin.writer.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Preconditions;;
import com.alibaba.fastjson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseWriter extends Writer {

    private final Logger logger = LoggerFactory.getLogger(HBaseWriter.class);

    private Table table;
    private int batchSize;
    //	private int rowkeyIndex = -1;
    private final List<Put> putList = new ArrayList<Put>();
    private String[] columns;
    //	private static final String ROWKEY = ":rowkey";
    private String[] rowkeyCol;
    private String namespace;
    private String tableName;

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
        batchSize = writerConfig.getInt(HBaseWriterProperties.BATCH_INSERT_SIZE, 1);

        Preconditions.checkNotNull(writerConfig.getString("fields"),
                "HBase writer required property: fields");
        columns = writerConfig.getString("fields").split(",");

        Preconditions.checkNotNull(writerConfig.getString("rowKey"),
                "HBase writer required property: rowKey");
        rowkeyCol = writerConfig.getString("rowKey").split(",");

        namespace = writerConfig.getString("namespace");

        tableName = writerConfig.getString("table");

//		Preconditions.checkNotNull(writerConfig.getString(HBaseWriterProperties.COLUMNS),
//				"HBase writer required property: zookeeper.columns");
//		columns = writerConfig.getString(HBaseWriterProperties.COLUMNS).split(",");
//		for (int i = 0, len = columns.length; i < len; i++) {
//			if (ROWKEY.equalsIgnoreCase(columns[i])) {
//				rowkeyIndex = i;
//				break;
//			}
//		}
//
//		if (rowkeyIndex == -1) {
//			throw new HDataException("Can not find :rowkey in columnsMapping of HBase Writer!");
//		}

        try {
            Preconditions.checkNotNull(writerConfig.getString(HBaseWriterProperties.TABLE),
                    "HBase writer required property: table");
            Connection conn = ConnectionFactory.createConnection(conf);
            createTable(conn, namespace, tableName, new String[]{"kf", "vf"});

            table = conn.getTable(TableName.valueOf(writerConfig.getString(HBaseWriterProperties.TABLE)));
        } catch (IOException e) {
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
            put.addColumn(Bytes.toBytes("vf"), Bytes.toBytes(columns[i - rowkeyCol.length]), Bytes.toBytes(o.toString()));
        }
        put.addColumn(Bytes.toBytes("kf"), Bytes.toBytes("kf"), Bytes.toBytes(JSON.toJSON(map).toString()));
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
                throw new HDataException(e);
            }
            putList.clear();
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
                throw new HDataException(e);
            }
            putList.clear();
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
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }
}
