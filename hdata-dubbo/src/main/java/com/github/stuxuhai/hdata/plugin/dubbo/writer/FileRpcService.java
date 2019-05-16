package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Record;
import com.merce.woven.data.rpc.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRpcService implements RpcCallable {

    private final Logger logger = LoggerFactory.getLogger(DataRpcService.class);

    private String tenantId;
    private String taskId;
    private String channelId;
    private JobContext jobContext;

    private static FileService fileService;

    @Override
    public void setup(String tenantId, String taskId, Configuration configuration) {
        fileService = connectWriterServer(configuration);
        if (fileService == null) {
            throw new RuntimeException("target server out of service ! ");
        }
        try {
            fileService.prepare(tenantId, taskId, configuration);
        } catch (Throwable e) {
            throw new RuntimeException("file service.prepare error", e);
        }
    }

    @Override
    public void prepare(String tenantId, String taskId, String channelId, JobContext jobContext) {
        this.channelId = channelId;
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.jobContext = jobContext;
    }

    @Override
    public void execute(Record record) {
        String orgPath = (String) record.get(0);
        String dstPath = (String) record.get(1);
        long size = (long) record.get(2);
        long modificationTime = (long) record.get(3);
        int ret = fileService.execute(tenantId, taskId, channelId, orgPath, dstPath, size, modificationTime);
        if (ret == -1) {
            jobContext.setWriterError(true);
            logger.error("task {} channel {} has error when flush data. the data server maybe lost.", taskId, channelId);
        }
    }

    @Override
    public void close(long total, boolean isLast) {
        if (fileService != null) {
            if (jobContext.isReaderError() || jobContext.isWriterError()) {
                fileService.onError(tenantId, taskId, channelId, new RuntimeException("reader or writer has error !"));
            } else {
                fileService.onFinish(tenantId, taskId, channelId, total, isLast);
            }
        }
    }

    public FileService connectWriterServer(Configuration writerConfig) {
        if (fileService == null) {
            synchronized (FileService.class) {
                if (fileService == null) {
                    ApplicationConfig application = new ApplicationConfig();
                    application.setName("hdata-dubbo-file-writer");
                    RegistryConfig registry = new RegistryConfig();
                    String protocol = writerConfig.getString("protocol");
                    registry.setProtocol(protocol);
                    registry.setClient("curatorx");

                    registry.setAddress(writerConfig.getString("address"));
                    registry.setUsername(writerConfig.getString("username"));
                    registry.setPassword(writerConfig.getString("password"));

                    ReferenceConfig<FileService> reference = new ReferenceConfig<FileService>();
                    reference.setApplication(application);
                    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
                    reference.setInterface(FileService.class);
                    reference.setTimeout(60 * 1000);

                    ConsumerConfig consumerConfig = new ConsumerConfig();
                    consumerConfig.setSticky(true);
                    consumerConfig.setTimeout(60 * 1000);
                    reference.setConsumer(consumerConfig);

                    try {
                        fileService = reference.get();
                    } catch (Exception e) {
                        logger.error("can't connect registry rpc-file-service", e);
                    }
                }
            }
        }
        return fileService;
    }
}
