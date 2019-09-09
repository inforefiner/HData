package com.inforefiner.hdata.plugin.reader.spooldir;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class SpoolDirReader extends Reader {
    private static final Logger logger = LoggerFactory.getLogger(SpoolDirReader.class);

    private Path dir = null;
    private String encoding = "UTF-8";
    private ExecutorService executorService;
    private WatchService watchService = null;

    /**
     * 0. 构建executorservice，使用reader并行度的线程池数目
     * 1. 列出当前目录文件，检查其是否需要处理，将每个文件提交给executorservice
     * 2. 注册watchservice，如果有新文件提交executorservice执行
     * 3. 处理文件的runnable逻辑中，首先将生成正在处理标示，阻止重复处理
     * 4. 按csv格式处理文件中每行数据，发送给recordcollector
     * 5. 处理完毕，重命名文件状态标示为完成
     */
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        String path = readerConfig.getString(SpoolDirReaderProperties.PATH);
        String parallelism = readerConfig.getString(SpoolDirReaderProperties.PARALLELISM);
        encoding = readerConfig.getString(SpoolDirReaderProperties.ENCODING, "UTF-8");
        try {
            executorService = Executors.newFixedThreadPool(Integer.parseInt(parallelism));
            watchService = FileSystems.getDefault().newWatchService();
            dir = Paths.get(path);
            if (Files.notExists(dir) || !Files.isDirectory(dir)) {
                logger.error("dir does not exist");
                throw new HDataException("dir does not exist");
            }
            dir.register(watchService, ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (NumberFormatException | IOException e) {
            logger.error("setup file system watch service failed", e);
            throw new HDataException(e);
        }
    }


    @Override
    public void execute(RecordCollector recordCollector) {
        try {
            handleExisting(recordCollector);
        } catch (IOException e) {
            logger.error("handle existing data file failed!", e);
            throw new HDataException(e);
        }
        while (!Thread.currentThread().isInterrupted()) {
            try {
                WatchKey key = watchService.poll(1, TimeUnit.MINUTES);
                if (key != null) {
                    key.pollEvents().stream().forEach(event -> {
                        logger.info("Event kind: {} . File affected: {} ", event.kind(), event.context());
                        Path name = ((WatchEvent<Path>) event).context();
                        Path child = dir.resolve(name);
                        if (event.kind() == ENTRY_CREATE) {
                            try {
                                handleSingle(child, 0, recordCollector);
                            } catch (IOException e) {
                                logger.error("handle data file error happened", e);
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }
                key.reset();
            } catch (InterruptedException e) {
                logger.error("watch service interrupted");
            }
        }
    }

    @Override
    public void close() {
        if (watchService != null) {
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

    private void handleExisting(RecordCollector recordCollector) throws IOException {
        Files.list(dir).filter(Files::isRegularFile).forEach(file -> {
            executorService.submit(() -> handleSingle(file, 0, recordCollector));
        });
    }

    private int handleSingle(Path path, int offset, RecordCollector recordCollector) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(path.toFile()), encoding));
        int current = 0;
        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            if (current > offset) {
                Record hdataRecord = new DefaultRecord(nextLine.length);
                for (int i = 0, len = nextLine.length; i < len; i++) {
                    hdataRecord.add(nextLine[i]);
                }
                recordCollector.send(hdataRecord);
            }
            current++;
        }
        return current;
    }
}
