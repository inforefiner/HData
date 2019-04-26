package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.Record;
import com.merce.woven.data.rpc.DataService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DataRpcService implements RpcCallable {

    private static final Logger logger = LogManager.getLogger(DataRpcService.class);

    private static int DEFAULT_BUFFER_SIZE = 5000;
    private static long MAX_FLUSH_PADDING_TIME = 1000 * 30;

    private static int bufferSize;
    private static long flushPaddingTime;

    private String tenantId;
    private String taskId;
    private String channelId;

    private static DataService dataService;

    private volatile boolean isClosed = false;

    private volatile boolean hasError = false;

    private volatile long lastFlushTime = 0l;

    private BlockingQueue<Object[]> bufferQueue;

    @Override
    public void setup(String tenantId, String taskId, Configuration configuration) {
        dataService = ConnectWriterServer(configuration);
        if (dataService == null) {
            throw new RuntimeException("target server out of service ! ");
        }
        try {
            dataService.prepare(tenantId, taskId, configuration);
        } catch (Exception e) {
            logger.error("can't connect data server", e);
            throw new RuntimeException("can't connect data server");
        }

        bufferSize = configuration.getInt("buffer.size", DEFAULT_BUFFER_SIZE);
        flushPaddingTime = configuration.getLong("flush.padding.time", MAX_FLUSH_PADDING_TIME);
    }

    @Override
    public void prepare(String tenantId, String taskId, String channelId) {
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.channelId = channelId;
        this.bufferQueue = new ArrayBlockingQueue(bufferSize, true);
        Thread t = new Thread(new DataSender());
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void execute(Record record) {
        if (hasError) {
            throw new RuntimeException("data service has error");
        }
        try {
            bufferQueue.put(record.strings());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
                if (bufferQueue.remainingCapacity() == 0 || (!bufferQueue.isEmpty() && (System.currentTimeMillis() - lastFlushTime) > flushPaddingTime)) {
                    try {
                        flushData();
                    } catch (Exception e) {
                        hasError = true;
                        logger.error(e);
                    }
                }
            }
        }
    }

    private int flushData() {
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
            hasError = true;
        } else {
            logger.info("task {} channel {} has flush {} records, size is {}, use time {} ms", taskId, channelId, list.size(), length, System.currentTimeMillis() - l);
        }
        return ret;
    }

    public static DataService ConnectWriterServer(Configuration writerConfig) {
        if (dataService == null) {
            synchronized (DataService.class) {
                if (dataService == null) {
                    ApplicationConfig application = new ApplicationConfig();
                    application.setName("hdata-dubbo-data-writer");
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
                    ReferenceConfig<DataService> reference = new ReferenceConfig<DataService>();
                    reference.setApplication(application);
                    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
                    reference.setInterface(DataService.class);
                    reference.setTimeout(60 * 1000);

                    ConsumerConfig consumerConfig = new ConsumerConfig();
                    consumerConfig.setSticky(true);
                    consumerConfig.setTimeout(60 * 1000);
                    reference.setConsumer(consumerConfig);

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
