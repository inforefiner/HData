package com.github.stuxuhai.hdata.core;

import com.github.stuxuhai.hdata.api.JobContext;
import com.google.common.base.Throwables;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.LoggerFactory;

public class RecordEventExceptionHandler implements ExceptionHandler<Object> {

    private final Disruptor<RecordEvent> disruptor;
    private final JobContext context;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RecordEventExceptionHandler.class);

    public RecordEventExceptionHandler(Disruptor<RecordEvent> disruptor, JobContext context) {
        this.disruptor = disruptor;
        this.context = context;
    }

    public void handleEventException(Throwable t, long sequence, Object event) {
        LOGGER.error(Throwables.getStackTraceAsString(t));
        context.setWriterError(true);
        disruptor.shutdown();
    }

    public void handleOnShutdownException(Throwable t) {
        LOGGER.error(Throwables.getStackTraceAsString(t));
        disruptor.shutdown();
    }

    public void handleOnStartException(Throwable t) {
        LOGGER.error(Throwables.getStackTraceAsString(t));
        disruptor.shutdown();
    }
}
