package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.inforefiner.europa.data.rpc.DataService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import com.inforefiner.europa.data.rpc.DataService;

/**
 * Created by joey on 2017/7/6.
 */
public class DubboWriter extends Writer {

    private static final Logger LOG = LogManager.getLogger(DubboWriter.class);

    private static AtomicInteger ChannelGenerator = new AtomicInteger();

    private static DataService dataService;

    private static int DEFAULT_BUFFER_SIZE = 5000;

    private static long MAX_FLUSH_PADDING_TIME = 1000 * 30;

    private volatile static AtomicBoolean serverPrepared = new AtomicBoolean(false);

    private volatile static AtomicInteger writerCounter = new AtomicInteger();

    private volatile static String cursorValue;

    private volatile AtomicLong counter = new AtomicLong(0l);

    private volatile boolean isClosed = false;

    private BlockingQueue<Object[]> bufferQueue;

    private String tenantId;

    private String taskId;

    private String channelId;

    @Override
    public void prepare(JobContext jobContext, PluginConfig writerConfig) {

        dataService = ConnectWriterServer(writerConfig);

        if (dataService == null) {
            throw new RuntimeException("target server out of service ! ");
        }

        tenantId = writerConfig.getString("tenantId");
        if (StringUtils.isBlank(tenantId)) {
            throw new RuntimeException("tenantId can't be null !");
        }

        taskId = writerConfig.getString("taskId");
        if (StringUtils.isBlank(taskId)) {
            throw new RuntimeException("taskId can't be null !");
        }

        channelId = "" + ChannelGenerator.getAndIncrement();

        if (serverPrepared.compareAndSet(false, true)) {
            cursorValue = jobContext.getJobConfig().getString("CursorValue");
            if (cursorValue != null) {
                writerConfig.setString("CursorValue", cursorValue);
            }
            try {
                dataService.prepare(tenantId, taskId, writerConfig);
            } catch (Exception e) {
                LOG.error("can't connect europa data server", e);
                throw new RuntimeException("can't connect europa data server");
            }
        }

        int bufferSize = writerConfig.getInt("bufferSize", DEFAULT_BUFFER_SIZE);

        bufferQueue = new ArrayBlockingQueue(bufferSize, true);

        Thread t = new Thread(new DataSender());
        t.start();

        int index = writerCounter.incrementAndGet();

        LOG.info("dubbo writer{} started, taskId={}, channelId={}, bufferSize={}", index, taskId, channelId, bufferSize);
    }

    public static DataService ConnectWriterServer(PluginConfig writerConfig) {
        if (dataService == null) {
            synchronized (DataService.class) {
                if (dataService == null) {
                    ApplicationConfig application = new ApplicationConfig();
                    application.setName("hdata-dubbo-writer");
                    RegistryConfig registry = new RegistryConfig();
                    registry.setProtocol(writerConfig.getString("protocol"));
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
                       LOG.error("can't connect registry server", e);
                    }
                }
            }

        }
        return dataService;
    }

    @Override
    public void execute(Record record) {
        if (record != null && record.size() > 0) {
            try {
                bufferQueue.put(record.strings());
                counter.incrementAndGet();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private volatile long LastFlushTime = 0l;

    class DataSender implements Runnable {
        @Override
        public void run() {
            while (!isClosed) {
                if (bufferQueue.remainingCapacity() == 0 || (!bufferQueue.isEmpty() && (System.currentTimeMillis() - LastFlushTime) > MAX_FLUSH_PADDING_TIME)) {
                    flushData();
                }
            }
        }
    }

    private void flushData() {
        try {
            LastFlushTime = System.currentTimeMillis();
            long l = System.currentTimeMillis();
            List list = new ArrayList();
            bufferQueue.drainTo(list);
            byte[] bytes = ByteUtil.toByteArray(list);
            int length = bytes.length;
            byte[] compressed = Lz4Util.compress(bytes, length);
            int ret = dataService.execute(tenantId, taskId, channelId, compressed, length, list.size());
            if (ret == -1) {
                LOG.error("task {} channel {} has error when flush data. the data server maybe lost.", taskId, channelId);
            } else {
                LOG.info("task {} channel {} has flush {} records, size is {}, use time {} ms", taskId, channelId, list.size(), length, System.currentTimeMillis() - l);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        LOG.info("task {} channel {} will be close, total sent {} records", taskId, channelId, counter.get());
        isClosed = true;
        if (dataService != null) {
            if (bufferQueue != null && bufferQueue.size() > 0) {
                flushData();
            }
            dataService.onFinish(tenantId, taskId, channelId, counter.get(), writerCounter.decrementAndGet() == 0);
        }
    }

}
