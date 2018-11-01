package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by joey on 2017/7/6.
 */
public class DubboWriter extends Writer {

    private static AtomicInteger ChannelGenerator = new AtomicInteger();

    private volatile static AtomicBoolean serverPrepared = new AtomicBoolean(false);

    private volatile static AtomicInteger writerCounter = new AtomicInteger();

    private volatile AtomicLong counter = new AtomicLong(0l);

    private String tenantId;

    private String taskId;

    private String channelId;

    private RpcCallable rpcService;

    @Override
    public void prepare(JobContext jobContext, PluginConfig writerConfig) {
        tenantId = writerConfig.getString("tenantId");
        if (StringUtils.isBlank(tenantId)) {
            throw new RuntimeException("tenantId can't be null !");
        }
        taskId = writerConfig.getString("taskId");
        if (StringUtils.isBlank(taskId)) {
            throw new RuntimeException("taskId can't be null !");
        }
        channelId = "" + ChannelGenerator.getAndIncrement();

        String serviceType = writerConfig.getString("ServiceType", "DTS");
        switch (serviceType) {
            case "DTS":
                rpcService = new DataRpcService();
                break;
            case "FTS":
                rpcService = new FileRpcService();
                break;
            default:
                throw new RuntimeException("non support the service type : " + serviceType);
        }
        String cursorValue = jobContext.getJobConfig().getString("CursorValue");
        if (cursorValue != null) {
            writerConfig.setString("CursorValue", cursorValue);
        }

        if (serverPrepared.compareAndSet(false, true)) {
            try {
                rpcService.setup(tenantId, taskId, writerConfig);
            } catch (Throwable e) {
                throw new RuntimeException("can't connect europa data server", e);
            }
        }

        rpcService.prepare(tenantId, taskId, channelId);

        writerCounter.incrementAndGet();
    }

    @Override
    public void execute(Record record) {
        if (record != null && record.size() > 0) {
            try {
                rpcService.execute(record);
                counter.incrementAndGet();
            } catch (Throwable e) {
                throw new HDataException(e);
            }
        }
    }

    @Override
    public void close() {
        try {
            rpcService.close(counter.get(), writerCounter.decrementAndGet() == 0);
        } catch (Throwable e) {
            throw new HDataException(e);
        }
    }
}
