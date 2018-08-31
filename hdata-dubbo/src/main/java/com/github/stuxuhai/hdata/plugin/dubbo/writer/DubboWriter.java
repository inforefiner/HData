package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
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

        String serviceType = writerConfig.getString("ServiceId", "DTS");

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

        writerCounter.incrementAndGet();

        if (serverPrepared.compareAndSet(false, true)) {
            String cursorValue = jobContext.getJobConfig().getString("CursorValue");
            if (cursorValue != null) {
                writerConfig.setString("CursorValue", cursorValue);
            }
            try {
                rpcService.prepare(tenantId, taskId, channelId, writerConfig);
            } catch (Throwable e) {
                throw new RuntimeException("can't connect europa data server", e);
            }
        }
    }

    @Override
    public void execute(Record record) {
        if (record != null && record.size() > 0) {
            try {
                rpcService.execute(record);
                counter.incrementAndGet();
            } catch (Throwable e) {
                throw new RuntimeException("an exception has been thrown when send data. ", e);
            }
        }
    }


    @Override
    public void close() {
        try {
            rpcService.close(counter.get(), writerCounter.decrementAndGet() == 0);
        } catch (Throwable e) {
            throw new RuntimeException("an exception has been thrown when close. ", e);
        }
    }
}
