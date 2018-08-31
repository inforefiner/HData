package com.github.stuxuhai.hdata.plugin.reader.ftp;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.ftp.FTPUtils;
import com.github.stuxuhai.hdata.ftp.FtpFile;
import com.github.stuxuhai.hdata.ftp.HdfsUtil;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FTPReader extends Reader {

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

        if (readerConfig.containsKey(FTPReaderProperties.SCHEMA)) {
            fields = new Fields();
            String[] tokens = readerConfig.getString(FTPReaderProperties.SCHEMA).split("\\s*,\\s*");
            for (String field : tokens) {
                fields.add(field);
            }
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        FTPClient ftpClient = null;
        try {
            ftpClient = FTPUtils.getFtpClient(host, port, username, password);
            for (FtpFile file : files) {
                String filePath = file.getPath();
                String _filePath = new String(filePath.getBytes("UTF-8"), "iso-8859-1");
                if ("hdfs".equalsIgnoreCase(readTo)) {
                    String fullPath = hdfsPath + filePath.replaceFirst(dir, "");
                    FileStatus _fs = HdfsUtil.getInstance().status(fullPath);
                    if (_fs != null && (!hdfsOverWrite || _fs.getModificationTime() >= file.getModificationTime())) {
                        continue;
                    }
                    InputStream is = ftpClient.retrieveFileStream(_filePath);
                    try {
                        OutputStream out = HdfsUtil.getInstance().create(fullPath);
                        IOUtils.copyBytes(is, out, 1024, true);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    Record record = new DefaultRecord(3);
                    record.add(fullPath);
                    record.add(file.getSize());
                    record.add(file.getModificationTime());
                    recordCollector.send(record);
                } else {
                    InputStream is = ftpClient.retrieveFileStream(_filePath);
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
                        if (currentRow >= startRow) {
                            String[] tokens = StringUtils.splitPreserveAllTokens(line, fieldsSeparator);
                            if (tokens.length >= fieldsCount) {
                                Record record = new DefaultRecord(tokens.length);
                                for (String field : tokens) {
                                    record.add(field);
                                }
                                recordCollector.send(record);
                            }
                        }
                    }
                    br.close();
                    is.close();
                }
                ftpClient.completePendingCommand();
            }
        } catch (Exception e) {
            throw new HDataException(e);
        } finally {
            FTPUtils.closeFtpClient(ftpClient);
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

}
