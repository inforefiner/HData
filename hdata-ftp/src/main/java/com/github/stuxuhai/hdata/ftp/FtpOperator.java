package com.github.stuxuhai.hdata.ftp;

import java.io.InputStream;
import java.util.List;

public interface FtpOperator {

    void connect(String host, int port, String username, String password);

    void list(List<FtpFile> files, String path, String filenameRegexp, boolean recursive);

    void close();

    void commit() throws Throwable;

    InputStream getInputStream(String path) throws Throwable;
}
