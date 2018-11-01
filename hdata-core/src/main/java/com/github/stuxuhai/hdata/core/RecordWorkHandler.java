package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.lmax.disruptor.WorkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordWorkHandler implements WorkHandler<RecordEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWorkHandler.class);

    private final Writer writer;
    private final JobContext context;
    private final PluginConfig writerConfig;
    private boolean writerPrepared;
    private final Metric metric;

    public RecordWorkHandler(Writer writer, JobContext context, PluginConfig writerConfig) {
        this.writer = writer;
        this.context = context;
        this.writerConfig = writerConfig;
        this.metric = context.getMetric();

        this.prepareWriter();
    }

    private void prepareWriter() {
        if (!writerPrepared) {
            context.declareOutputFields();
            Thread.currentThread().setContextClassLoader(writer.getClass().getClassLoader());
            writer.prepare(context, writerConfig);
            writerPrepared = true;
            if (metric.getWriterStartTime() == 0) {
                metric.setWriterStartTime(System.currentTimeMillis());
            }
        }
    }

    @Override
    public void onEvent(RecordEvent event) {
        try {
            writer.execute(event.getRecord());
            metric.getWriteCount().incrementAndGet();
        } catch (Throwable e) {
            context.setWriterError(true);
            throw new HDataException(e);
        }
    }
}
