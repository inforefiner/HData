package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.Record;
import com.inforefiner.europa.data.rpc.FileService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileRpcService implements RpcCallable {

    private static final Logger logger = LogManager.getLogger(DataRpcService.class);

    private String tenantId;
    private String taskId;
    private String channelId;

    private static FileService fileService;

    @Override
    public void prepare(String tenantId, String taskId, String channelId, Configuration configuration) {
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.channelId = channelId;

        fileService = ConnectWriterServer(configuration);
        if (fileService == null) {
            throw new RuntimeException("target server out of service ! ");
        }

        try {
            fileService.prepare(tenantId, taskId, configuration);
        } catch (Exception e) {
            logger.error("can't connect europa data server", e);
            throw new RuntimeException("can't connect europa data server");
        }
    }

    @Override
    public void execute(Record record) {
        String path = (String) record.get(0);
        long size = (long) record.get(1);
        long modificationTime = (long) record.get(2);
        int ret = fileService.execute(tenantId, taskId, channelId, path, size, modificationTime);
        if (ret == -1) {
            logger.error("task {} channel {} has error when flush data. the data server maybe lost.", taskId, channelId);
        }
    }

    @Override
    public void close(long total, boolean isLast) {
        if (fileService != null) {
            fileService.onFinish(tenantId, taskId, channelId, total, isLast);
        }
    }

    public static FileService ConnectWriterServer(Configuration writerConfig) {
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
