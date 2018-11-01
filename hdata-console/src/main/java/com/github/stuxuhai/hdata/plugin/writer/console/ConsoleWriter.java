package com.github.stuxuhai.hdata.plugin.writer.console;

import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.exception.HDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;

public class ConsoleWriter extends Writer {

    private static final Logger LOG = LogManager.getLogger(ConsoleWriter.class);

    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private AtomicLong counter = new AtomicLong();

    private boolean isDebug;

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        LOG.info("ConsoleWriter : " + Thread.currentThread().getName() + " prepare writing ...");
        String cursorValue = context.getJobConfig().getString("CursorValue");
        LOG.info("CursorValue : " + cursorValue);
        isDebug = writerConfig.getBoolean("debug", false);
    }

    @Override
    public void execute(Record record) {
        long count = counter.incrementAndGet();
        if (count % 2000 == 0) {
            LOG.info("ConsoleWriter : " + Thread.currentThread().getName() + " has write " + counter.get() + " records ...");
            throw new HDataException();
        }
        if (isDebug) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0, len = record.size(); i < len; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                Object obj = record.get(i);
                if (obj instanceof Timestamp) {
                    sb.append(dateFormat.format(obj));
                } else {
                    sb.append(obj);
                }
            }
            sb.append("}");
            LOG.info(Thread.currentThread().getName() + " : " + sb.toString());
        }
    }


    @Override
    public void close() {
        LOG.info("ConsoleWriter : " + Thread.currentThread().getName() + " write over ! records = " + counter.get());
    }
}
