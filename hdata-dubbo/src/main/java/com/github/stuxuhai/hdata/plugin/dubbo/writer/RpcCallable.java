package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import com.github.stuxuhai.hdata.api.Configuration;
import com.github.stuxuhai.hdata.api.Record;

public interface RpcCallable {

    void prepare(String tenantId, String taskId, String channelId, Configuration configuration) throws Throwable;

    void execute(Record record) throws Throwable;

    void close(long total, boolean isLast) throws Throwable;
}
