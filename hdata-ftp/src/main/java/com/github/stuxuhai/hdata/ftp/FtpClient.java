package com.github.stuxuhai.hdata.ftp;

import org.apache.commons.net.ftp.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FtpClient implements FtpOperator {

    private FTPClient client;

    @Override
    public void connect(String host, int port, String username, String password) {
        try {
            String LOCAL_CHARSET = "GB18030";
            client = new FTPClient();
            client.connect(host, port);
            // 检测服务器是否支持UTF-8编码，如果支持就用UTF-8编码，否则就使用本地编码GB18030
            if (FTPReply.isPositiveCompletion(client.sendCommand("OPTS UTF8", "ON"))) {
                LOCAL_CHARSET = "UTF-8";
            }
            client.setControlEncoding(LOCAL_CHARSET);
            client.login(username, password);
            client.setBufferSize(1024 * 1024);
            client.enterLocalPassiveMode();
            client.setFileType(FTP.BINARY_FILE_TYPE);
            client.setControlKeepAliveTimeout(60);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void list(List<FtpFile> files, String path, String filenameRegexp, boolean recursive) {
        try {
            String _path = new String(path.getBytes("UTF-8"), "iso-8859-1");
            for (FTPFile ftpFile : client.listFiles(_path)) {
                if (ftpFile.isFile()) {
                    if (Pattern.matches(filenameRegexp, ftpFile.getName())) {
                        files.add(new FtpFile(path + "/" + ftpFile.getName(), ftpFile.getSize(), ftpFile.getTimestamp().getTimeInMillis()));
                    }
                } else if (recursive && ftpFile.isDirectory()) {
                    list(files, path + "/" + ftpFile.getName(), filenameRegexp, recursive);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public InputStream getInputStream(String path) throws Throwable {
        return client.retrieveFileStream(path);
    }

    @Override
    public void close() {
        if (client != null) {
            try {
//                client.completePendingCommand();
                client.disconnect();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

//        try {
//            FTPSClient client = getFtpsClient("192.168.1.189", 22, "merce", "merce");
//            FTPFile[] files = client.listFiles("/app/streaming/runner");
//            for (FTPFile f : files) {
//                System.out.println(f.toFormattedString());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String filenameRegexp = "([\\w\\W]*).csv";
//        String[] filenames = new String[]{"ftp_demo.csv", "demo.csv"};
//
//        for (String name : filenames) {
//            System.out.println(Pattern.matches(filenameRegexp, name));
//        }
    }
}
