package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.Record;
import com.inforefiner.europa.data.rpc.DataService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DataRpcService implements RpcCallable {

    private static final Logger logger = LogManager.getLogger(DataRpcService.class);

    private String tenantId;
    private String taskId;
    private String channelId;

    private static DataService dataService;

    private volatile boolean isClosed = false;

    private volatile long lastFlushTime = 0l;

    private static int DEFAULT_BUFFER_SIZE = 5000;

    private static long MAX_FLUSH_PADDING_TIME = 1000 * 30;

    private BlockingQueue<Object[]> bufferQueue;

    @Override
    public void prepare(String tenantId, String taskId, String channelId, Configuration configuration) {
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.channelId = channelId;

        dataService = ConnectWriterServer(configuration);
        if (dataService == null) {
            throw new RuntimeException("target server out of service ! ");
        }

        try {
            dataService.prepare(tenantId, taskId, configuration);
        } catch (Exception e) {
            logger.error("can't connect europa data server", e);
            throw new RuntimeException("can't connect europa data server");
        }

        int bufferSize = configuration.getInt("bufferSize", DEFAULT_BUFFER_SIZE);

        bufferQueue = new ArrayBlockingQueue(bufferSize, true);

        Thread t = new Thread(new DataSender());
        t.start();
    }

    @Override
    public void execute(Record record) throws Throwable {
        bufferQueue.put(record.strings());
    }

    @Override
    public void close(long total, boolean isLast) {
        isClosed = true;
        if (dataService != null) {
            if (bufferQueue != null && bufferQueue.size() > 0) {
                flushData();
            }
            dataService.onFinish(tenantId, taskId, channelId, total, isLast);
        }
    }

    class DataSender implements Runnable {
        @Override
        public void run() {
            while (!isClosed) {
                if (bufferQueue.remainingCapacity() == 0 || (!bufferQueue.isEmpty() && (System.currentTimeMillis() - lastFlushTime) > MAX_FLUSH_PADDING_TIME)) {
                    flushData();
                }
            }
        }
    }

    private void flushData() {
        try {
            lastFlushTime = System.currentTimeMillis();
            long l = System.currentTimeMillis();
            List list = new ArrayList();
            bufferQueue.drainTo(list);
            byte[] bytes = ByteUtil.toByteArray(list);
            int length = bytes.length;
            byte[] compressed = Lz4Util.compress(bytes, length);
            int ret = dataService.execute(tenantId, taskId, channelId, compressed, length, list.size());
            if (ret == -1) {
                logger.error("task {} channel {} has error when flush data. the data server maybe lost.", taskId, channelId);
            } else {
                logger.info("task {} channel {} has flush {} records, size is {}, use time {} ms", taskId, channelId, list.size(), length, System.currentTimeMillis() - l);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataService ConnectWriterServer(Configuration writerConfig) {
        if (dataService == null) {
            synchronized (DataService.class) {
                if (dataService == null) {
                    ApplicationConfig application = new ApplicationConfig();
                    application.setName("hdata-dubbo-data-writer");
                    RegistryConfig registry = new RegistryConfig();
                    String protocol = writerConfig.getString("protocol");
                    registry.setProtocol(protocol);
                    registry.setClient("curatorx");

                    registry.setAddress(writerConfig.getString("address"));
                    registry.setUsername(writerConfig.getString("username"));
                    registry.setPassword(writerConfig.getString("password"));

                    ReferenceConfig<DataService> reference = new ReferenceConfig<DataService>();
                    reference.setApplication(application);
                    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
                    reference.setInterface(DataService.class);
                    reference.setTimeout(60 * 1000);

                    try {
                        dataService = reference.get();
                    } catch (Exception e) {
                        logger.error("can't connect registry rpc-data-service", e);
                    }
                }
            }
        }
        return dataService;
    }
}
