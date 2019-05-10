package com.github.stuxuhai.hdata.core;

import java.util.concurrent.Callable;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Reader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReaderWorker implements Callable<Integer> {

    private final Logger logger = LogManager.getLogger(ReaderWorker.class);

    private final Reader reader;
    private final JobContext context;
    private final DefaultRecordCollector rc;
    private final PluginConfig readerConfig;

    public ReaderWorker(Reader reader, JobContext context, PluginConfig readerConfig, DefaultRecordCollector rc) {
        this.reader = reader;
        this.context = context;
        this.rc = rc;
        this.readerConfig = readerConfig;
    }

    @Override
    public Integer call() {
        Thread.currentThread().setContextClassLoader(reader.getClass().getClassLoader());
        reader.prepare(context, readerConfig);
        try {
            reader.execute(rc);
        } catch (Throwable e) {
            context.setReaderError(true);
            context.setReaderErrorMsg(e.getMessage());
        } finally {
            reader.close();
        }
        return 0;
    }

}
