package com.inforefiner.hdata.plugin.reader.spooldir;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.exception.HDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class SpoolDirReader extends Reader {
    private static final Logger logger = LoggerFactory.getLogger(SpoolDirReader.class);

    private Path dir = null;
    private ExecutorService executorService;
    private WatchService watchService = null;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        String path = readerConfig.getString(SpoolDirReaderProperties.PATH);
        executorService = Executors.newFixedThreadPool(1);
        try {
            watchService = FileSystems.getDefault().newWatchService();
            dir = Paths.get(path);
            if(Files.notExists(dir) || !Files.isDirectory(dir)) {
                logger.error("dir does not exist");
                throw new HDataException("dir does not exist");
            }
            dir.register(watchService, ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (IOException e) {
            logger.error("setup file system watch service failed");
            throw new HDataException(e);
        }
    }


    @Override
    public void execute(RecordCollector recordCollector) {
        executorService.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    WatchKey key = watchService.poll(1, TimeUnit.MINUTES);
                    if(key != null) {
                        key.pollEvents().stream().forEach(event -> {
                                    logger.info("Event kind: {} . File affected: {} ", event.kind(), event.context() );
                                    Path name = ((WatchEvent<Path>)event).context();
                                    Path child = dir.resolve(name);
                                    if(event.kind() == ENTRY_CREATE) {

                                    }
                                });
                    }
                    key.reset();
                } catch (InterruptedException e) {
                    logger.error("watch service interrupted");
                }
            }
        });
    }

    @Override
    public void close() {
        if(watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                logger.error("watch service close failed", e);
            }
        }
    }

    @Override
    public Splitter newSplitter() {
        return null;
    }
}
