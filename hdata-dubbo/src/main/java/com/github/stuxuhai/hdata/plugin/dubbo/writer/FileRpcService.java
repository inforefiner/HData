package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.NacosNamingService;
import org.apache.commons.lang.StringUtils;
import org.apache.dubbo.config.*;
import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.Record;
import com.merce.woven.data.rpc.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FileRpcService implements RpcCallable {

    private final Logger logger = LoggerFactory.getLogger(FileRpcService.class);

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
            if (jobContext.isReaderError()) {
                fileService.onError(tenantId, taskId, channelId, new RuntimeException("reader has error !"));
            } else if (jobContext.isWriterError()) {
                fileService.onError(tenantId, taskId, channelId, new RuntimeException("writer has error !"));
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
                    application.setQosEnable(false);
                    RegistryConfig registry = new RegistryConfig();
                    String protocol = writerConfig.getString("protocol", "nacos");
                    registry.setProtocol(protocol);
                    registry.setAddress(writerConfig.getString("address"));

                    ReferenceConfig<FileService> reference = new ReferenceConfig<>();
                    reference.setApplication(application);
                    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
                    reference.setInterface(FileService.class);
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
                        fileService = reference.get();
                    } catch (Exception e) {
                        logger.error("can't connect registry rpc-file-service", e);
                    }
                }
            }
        }
        return fileService;
    }

    private String getUrl(String address, String clusterId) {
        logger.info("address: {}, cluster id: {}", address, clusterId);
        String serviceName = "providers:" + FileService.class.getName() + "::";
        try {
            NacosNamingService namingService = new NacosNamingService(address);
            List<Instance> list = new ArrayList<>();

            if (StringUtils.isNotEmpty(clusterId)) {
                for (Instance instance : namingService.getAllInstances(serviceName)) {
                    if (instance.getMetadata().containsKey("clusterId")) {
                        if (clusterId.equals(instance.getMetadata().get("clusterId"))) {
                            list.add(instance);
                        }
                    }
                }
            } else {
                list = namingService.getAllInstances(serviceName);
            }

            if (list != null && list.size() > 0) {
                int index = ThreadLocalRandom.current().nextInt(list.size());
                Instance instance = list.get(index);
                return instance.getIp() + ":" + instance.getPort();

            } else {
                logger.error("can't get available service for " + serviceName);
                throw new RuntimeException("can't get available service for " + serviceName);
            }
        } catch (Exception e) {
            logger.error("get available service exception for " + serviceName, e);
            throw new RuntimeException("get available service exception for " + serviceName);
        }
    }
}
