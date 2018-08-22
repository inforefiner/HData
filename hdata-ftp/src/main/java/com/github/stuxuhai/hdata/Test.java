package com.github.stuxuhai.hdata;

import com.github.stuxuhai.hdata.ftp.FTPUtils;
import org.apache.commons.net.ftp.FTPClient;

import java.util.ArrayList;
import java.util.List;

public class Test {


    public static void main(String[] args) throws Throwable {
        String host = "node2";
        int port = 21;
        String username = "ftp_test1";
        String password = "ftp_test1";
        String filenameRegexp = ".*";
        String dir = "pub";
        boolean recursive = true;
        FTPClient ftpClient =FTPUtils.getFtpClient(host, port, username, password);
        List<String> files = new ArrayList<String>();
        FTPUtils.listFile(files, ftpClient, dir, filenameRegexp, recursive);
        System.out.println(files);
    }
}
