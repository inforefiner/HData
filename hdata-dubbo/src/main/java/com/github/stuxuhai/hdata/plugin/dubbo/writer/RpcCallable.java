package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.Record;

public interface RpcCallable {

    void setup(String tenantId, String taskId, Configuration configuration) throws Throwable;

    void prepare(String tenantId, String taskId, String channelId);

    void execute(Record record) throws Throwable;

    void close(long total, boolean isLast) throws Throwable;
}
