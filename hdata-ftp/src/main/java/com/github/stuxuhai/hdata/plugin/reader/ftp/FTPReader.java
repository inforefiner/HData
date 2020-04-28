package com.github.stuxuhai.hdata.plugin.reader.ftp;

import com.github.stuxuhai.hdata.api.Reader;
import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.ftp.*;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FTPReader extends Reader {

    private final Logger LOG = LoggerFactory.getLogger(FTPReader.class);

    private Fields fields;
    private String host;
    private int port;
    private String username;
    private String password;
    private String fieldsSeparator;
    private String encoding;
    private int fieldsCount;
    private int startRow;
    private List<FtpFile> files = new ArrayList();

    private String dir;
    private String readTo;
    private String hdfsPath;
    private boolean hdfsOverWrite;
    private String hadoopConfPath;
//    private String clusterId;
//    private String hdfsUrl;
    private boolean secure;
    private boolean skipHeader;
    private String httpUrl;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        host = readerConfig.getString(FTPReaderProperties.HOST);
        port = readerConfig.getInt(FTPReaderProperties.PORT, 21);
        username = readerConfig.getString(FTPReaderProperties.USERNAME, "anonymous");
        password = readerConfig.getString(FTPReaderProperties.PASSWORD, "");
        fieldsSeparator = StringEscapeUtils
                .unescapeJava(readerConfig.getString(FTPReaderProperties.FIELDS_SEPARATOR, "\t"));
        encoding = readerConfig.getString(FTPReaderProperties.ENCODING, "UTF-8");
        files = (List<FtpFile>) readerConfig.get(FTPReaderProperties.FILES);
        fieldsCount = readerConfig.getInt(FTPReaderProperties.FIELDS_COUNT, 0);
        startRow = readerConfig.getInt(FTPReaderProperties.START_ROW, 1);

        dir = readerConfig.getString(FTPReaderProperties.DIR);

        readTo = readerConfig.getString(FTPReaderProperties.READ_TO, "");
        hdfsPath = readerConfig.getString(FTPReaderProperties.HDFS_PATH, "");
        hdfsOverWrite = readerConfig.getBoolean(FTPReaderProperties.HDFS_OVERWRITE, true);
        hadoopConfPath = readerConfig.getString(FTPReaderProperties.HADOOP_CONF_PATH, "");

        httpUrl = readerConfig.getString(FTPReaderProperties.HTTP_URL, "");

        secure = readerConfig.getBoolean(FTPReaderProperties.SECURE, false);

        skipHeader = readerConfig.getBoolean(FTPReaderProperties.SKIP_HEADER, false);

        if (readerConfig.containsKey(FTPReaderProperties.SCHEMA)) {
            fields = new Fields();
            String[] tokens = readerConfig.getString(FTPReaderProperties.SCHEMA).split("\\s*,\\s*");
            fields.addAll(Arrays.asList(tokens));
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        FtpOperator operator = null;
        if (secure) {
            operator = new FtpsClient();
        } else {
            operator = new FtpClient();
        }
        String tmpDir = "/tmp/hadoop_conf_" + System.currentTimeMillis() + "/";
        try {
            operator.connect(host, port, username, password);
            HdfsUtil hdfsUtil = HdfsUtil.getInstance(hadoopConfPath, tmpDir);
            for (FtpFile file : files) {
                String filePath = file.getPath();
                String _filePath = new String(filePath.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
                if ("hdfs".equalsIgnoreCase(readTo)) {
                    String fullPath = hdfsPath + filePath.replaceFirst(dir, "");
                    FileStatus _fs = hdfsUtil.status(fullPath);
                    if (_fs != null) {
                        if ((!hdfsOverWrite || _fs.getModificationTime() >= file.getModificationTime())) {
                            continue;
                        } else {
                            hdfsUtil.delete(fullPath);
                        }
                    }
                    InputStream is = null;
                    try {
                        is = operator.getInputStream(filePath);
                    } catch (Throwable e) {
                        LOG.error("file " + filePath + " can't get by ftp client.", e);
                        continue;
                    }
                    if (is == null) {
                        continue;
                    }
                    try {
                        String pendingPath = fullPath + ".pending";
                        hdfsUtil.delete(pendingPath);
                        OutputStream out = hdfsUtil.create(pendingPath);
                        LOG.info("transmitting file {}", filePath);
                        long l = System.currentTimeMillis();
                        IOUtils.copyBytes(is, out, 1024, true);
                        LOG.info("file {} has been transmitted, use time {} sec.", filePath, (System.currentTimeMillis() - l) / 1000);
                        hdfsUtil.rename(pendingPath, fullPath);
                    } catch (Throwable e) {
                        LOG.error("can't write to hdfs : " + fullPath, e);
                        continue;
                    } finally {
                        try {
                            operator.commit();
                        } catch (Throwable e) {
                            LOG.error("file commit error : ", e);
                        }
                    }
                    Record record = new DefaultRecord(4);
                    record.add(filePath);
                    record.add(fullPath);
                    record.add(file.getSize());
                    record.add(file.getModificationTime());
                    recordCollector.send(record);
                } else if ("http".equalsIgnoreCase(readTo)) {
                    InputStream is;
                    try {
                        is = operator.getInputStream(_filePath);
                    } catch (Throwable e) {
                        LOG.error("file " + filePath + " can't get by ftp client.", e);
                        continue;
                    }
                    if (is == null) {
                        continue;
                    }
                    File tmpFile = File.createTempFile("tmp_", "");
                    tmpFile.deleteOnExit();
                    FileOutputStream fos = new FileOutputStream(tmpFile);
                    IOUtils.copyBytes(is, fos, 1024, true);
                    String fullPath = hdfsPath + filePath.replaceFirst(dir, "");
                    FileSender sender = new FileSender(httpUrl, tmpFile.getPath(), fullPath);
                    if (!sender.exists(file.getSize(), file.getModificationTime())) {
                        sender.send();
                        Record record = new DefaultRecord(4);
                        record.add(filePath);
                        record.add(fullPath);
                        record.add(file.getSize());
                        record.add(file.getModificationTime());
                        recordCollector.send(record);
                    }
                } else {
                    InputStream is;
                    try {
                        is = operator.getInputStream(_filePath);
                    } catch (Throwable e) {
                        LOG.error("file " + filePath + " can't get by ftp client.", e);
                        continue;
                    }
                    if (is == null) {
                        continue;
                    }
                    BufferedReader br = null;
                    if (filePath.endsWith(".gz")) {
                        GZIPInputStream gzin = new GZIPInputStream(is);
                        br = new BufferedReader(new InputStreamReader(gzin, encoding));
                    } else {
                        br = new BufferedReader(new InputStreamReader(is, encoding));
                    }
                    String line = null;
                    long currentRow = 0;
                    while ((line = br.readLine()) != null) {
                        currentRow++;
                        if (skipHeader && currentRow == 1) {
                            continue;
                        }
                        String[] tokens = StringUtils.splitPreserveAllTokens(line, fieldsSeparator);
                        if (tokens.length >= fieldsCount) {
                            Record record = new DefaultRecord(tokens.length);
                            for (String field : tokens) {
                                record.add(field);
                            }
                            recordCollector.send(record);
                        }
                    }
                    br.close();
                    is.close();
                }
            }
        } catch (Throwable e) {
            LOG.info("ftp reader error:" + e.getMessage());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteFileAll(new File(tmpDir))));
            throw new HDataException(e);
        } finally {
            operator.close();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteFileAll(new File(tmpDir))));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

    @Override
    public Splitter newSplitter() {
        return new FTPSplitter();
    }

    public static void deleteFileAll(File file) {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File value : files) {
                    if (value.isDirectory()) {
                        deleteFileAll(value);
                    } else {
                        value.delete();
                    }
                }
            }
            file.delete();
        }
    }

}
