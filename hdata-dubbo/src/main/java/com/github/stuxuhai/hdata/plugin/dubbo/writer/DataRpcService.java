package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Record;
import com.google.common.base.Preconditions;
import com.inforefiner.hdata.reporter.DubboReporter;
import com.merce.woven.data.rpc.DataService;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.apache.commons.lang.StringUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DataRpcService implements RpcCallable {

    private final Logger logger = LoggerFactory.getLogger(DataRpcService.class);

    private static final String SIZE_KEY = "L";

    private static int DEFAULT_BUFFER_SIZE = 5000;
    private static long MAX_FLUSH_PADDING_TIME = 1000 * 30;

    private static int bufferSize;
    private static long flushPaddingTime;

    private String tenantId;
    private String taskId;
    private String channelId;
    private JobContext jobContext;

    private static DataService dataService;

    private volatile boolean isClosed = false;

    private volatile long lastFlushTime = 0L;

    // chronicle queue settings
    private Path queueDir;
    private ChronicleQueue eventQueue;
    private ExcerptTailer tailer;

    // pointer for read/write offset in event queue
    private AtomicInteger readPointer = new AtomicInteger(0);
    private AtomicInteger writePointer = new AtomicInteger(0);

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter dubboReceive = metrics.meter("dubboReceive");
    private final Meter dubboWrite = metrics.meter("dubboWrite");

    @Override
    public void setup(String tenantId, String taskId, Configuration configuration) {

        bufferSize = configuration.getInt("buffer.size", DEFAULT_BUFFER_SIZE);
        flushPaddingTime = configuration.getLong("flush.padding.time", MAX_FLUSH_PADDING_TIME);

        dataService = connectWriterServer(configuration);
        if (dataService == null) {
            throw new RuntimeException("target server out of service ! ");
        }
        try {
            dataService.prepare(tenantId, taskId, configuration);
        } catch (Throwable e) {
            throw new RuntimeException("data service prepare error", e);
        }
    }

    @Override
    public void prepare(String tenantId, String taskId, String channelId, JobContext jobContext) {
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.channelId = channelId;
        this.jobContext = jobContext;
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger("com.github.stuxuhai.hdata.plugin.dubbo.writer.DataRpcService"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);

        final DubboReporter dubboReporter = DubboReporter.forRegistry(metrics)
                .outputTo(dataService)
                .withTenantId(tenantId)
                .withTaskId(taskId)
                .withChannelId(channelId)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        dubboReporter.start(10, TimeUnit.SECONDS);

        try {
            this.queueDir = Files.createTempDirectory("hdata-dubbo-data-writer-queue" + taskId + System.currentTimeMillis());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteFileAll(queueDir.toFile())));
        } catch (IOException e) {
            throw new RuntimeException("data service prepare error", e);
        }


        eventQueue = ChronicleQueue.singleBuilder(this.queueDir).rollCycle(RollCycles.HOURLY).storeFileListener(new StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                logger.info("{} onAcquired {}", queueDir, file.getAbsolutePath());
            }

            @Override
            public void onReleased(int cycle, File file) {
                logger.info("{} onReleased {}", queueDir, file.getAbsolutePath());
                long lastModified = file.lastModified();
                File parent = file.getParentFile();
                File[] files = parent.listFiles();
                if (files != null) {
                    for (File f : files) {
                        if (f.isFile() && f.lastModified() < lastModified) {
                            if (f.delete()) {
                                logger.info("{} deleted {}", queueDir, file.getAbsolutePath());
                            } else {
                                logger.info("{} delete {} failed", queueDir, file.getAbsolutePath());
                            }
                        }
                    }
                }
            }
        }).build();

        this.tailer = eventQueue.createTailer();
        tailer.toStart();

        Thread t = new Thread(new DataSender());
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void execute(Record record) {
        final ExcerptAppender appender = eventQueue.acquireAppender();
        try (final DocumentContext dc = appender.writingDocument()) {
            String[] values = record.strings();
            Wire wire = dc.wire();
            int length = values.length;
            wire.write(SIZE_KEY).int32(length);
            for (String value : values) {
                wire.write().text(value);
            }
            int wrote = writePointer.incrementAndGet();
            dubboReceive.mark();
            logger.debug("enqueue {} records", wrote);
        } catch (Exception e) {
            logger.error("enqueue {} error", queueDir, e);
            throw new RuntimeException(queueDir + " store event error", e);
        }
    }

    @Override
    public void close(long total, boolean isLast) {
        isClosed = true;
        if (dataService != null) {
            if (eventQueue != null && !eventQueue.isClosed()) {
                int toRead = writePointer.get() - readPointer.get();
                if (toRead > 0) {
                    flushData();
                }

                try {
                    eventQueue.close();
                } catch (Exception e) {
                    logger.error("close {} error {}", queueDir, e);
                }
            }

            if (jobContext.isReaderError() || jobContext.isWriterError()) {
                dataService.onError(tenantId, taskId, channelId, new RuntimeException("reader or writer has error !"));
            } else {
                dataService.onFinish(tenantId, taskId, channelId, total, isLast);
            }
        }
    }

    class DataSender implements Runnable {
        @Override
        public void run() {
            while (!isClosed) {
                if ((writePointer.get() - readPointer.get()) > bufferSize || (System.currentTimeMillis() - lastFlushTime) > flushPaddingTime) {
                    try {
                        flushData();
                    } catch (Throwable e) {
                        jobContext.setWriterError(true);
                        logger.error("flush data error", e);
                    }
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private synchronized int flushData() {
        lastFlushTime = System.currentTimeMillis();
        long l = System.currentTimeMillis();
        ArrayList<String[]> list = new ArrayList<>(bufferSize);
        Preconditions.checkNotNull(tailer);
        while (list.size() < bufferSize) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.isPresent()) {
                    Wire wire = dc.wire();
                    int size = wire.read(SIZE_KEY).int32();
                    String[] values = new String[size];
                    for (int j = 0; j < size; j++) {
                        values[j] = wire.read().text();
                    }
                    list.add(values);
                    int read = readPointer.incrementAndGet();
                    logger.debug("dequeue {} records", read);
                } else {
                    break;
                }

            } catch (Exception e) {
                logger.error("read {} failed", queueDir, e);
                if (e.getMessage().startsWith("Queue is closed")) {
                    logger.info("exit queue read loop!");
                    break;
                }
            }
        }

        int ret = 0;
        int toSendCount = list.size();
        if (toSendCount > 0) {
            byte[] bytes = KryoUtils.writeToByteBuffer(list);
            logger.info("flushData() serialize {} bytes", bytes.length);
            int length = bytes.length;
            ret = dataService.execute(tenantId, taskId, channelId, bytes, length, toSendCount);
            dubboWrite.mark(toSendCount);
            if (ret == -1) {
                jobContext.setWriterError(true);
                logger.error("task {} channel {} has error when flush data. the data server maybe lost.", taskId, channelId);
            } else {
                logger.debug("task {} channel {} has flush {} records, size is {}, use time {} ms", taskId, channelId,
                        toSendCount, length, System.currentTimeMillis() - l);
            }
        }
        return ret;
    }


    public static void deleteFileAll(File file) {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File value : files) {
                    if (value.isDirectory()) {
                        deleteFileAll(value);
                    } else {
                        value.delete();
                    }
                }
            }
            file.delete();
        }
    }

    private DataService connectWriterServer(Configuration writerConfig) {
        if (dataService == null) {
            synchronized (DataService.class) {
                if (dataService == null) {
                    ApplicationConfig application = new ApplicationConfig();
                    application.setName("hdata-dubbo-data-writer");
                    application.setQosEnable(false);
                    RegistryConfig registry = new RegistryConfig();
                    String protocol = writerConfig.getString("protocol", "consul");
                    registry.setProtocol(protocol);

                    registry.setAddress(writerConfig.getString("address"));
                    try {
                        File file = File.createTempFile("dubbo-" + System.currentTimeMillis(), ".cache");
                        file.deleteOnExit();
                        registry.setFile(file.getPath());
                        logger.info("cached registry file in " + file.getPath());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    ReferenceConfig<DataService> reference = new ReferenceConfig<>();
                    reference.setApplication(application);
                    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
                    reference.setInterface(DataService.class);
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
                        dataService = reference.get();
                    } catch (Throwable e) {
                        logger.error("can't connect registry rpc-data-service", e);
                    }
                }
            }
        }
        return dataService;
    }

    /**
     * 检查哪个datahub存活并且和clusterI匹配
     * @param address
     * @param clusterId
     * @return
     */
    private String getUrl(String address, String clusterId) {
        logger.info("address: {}, cluster id: {}", address, clusterId);
        String serviceName = "providers:"+DataService.class.getName()+"::";
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
