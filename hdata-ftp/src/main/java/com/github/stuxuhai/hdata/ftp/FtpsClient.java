package com.github.stuxuhai.hdata.ftp;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Pattern;

import com.jcraft.jsch.*;

public class FtpsClient implements FtpOperator {

    private ChannelSftp sftp;

    private Channel channel;

    private Session session;

    @Override
    public void connect(String host, int port, String username, String password) {
        try {
            JSch jsch = new JSch();
            jsch.getSession(username, host, port);
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            session.setConfig(sshConfig);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            sftp = (ChannelSftp) channel;
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void list(List<FtpFile> files, String path, String filenameRegexp, boolean recursive) {
        try {
            Vector<?> vector = sftp.ls(path);
            for (Object item : vector) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) item;
                SftpATTRS attrs = entry.getAttrs();
                String name = entry.getFilename();
                if (!".".equals(name) && !"..".equals(name)) {
                    if (attrs.isDir()) {
                        if (recursive) {
                            list(files, path + "/" + name, filenameRegexp, recursive);
                        }
                    } else {
                        if (Pattern.matches(filenameRegexp, name)) {
                            files.add(new FtpFile(path + "/" + name, attrs.getSize(), attrs.getMTime()));
                        }
                    }
                }

            }
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }

    @Override
    public InputStream getInputStream(String path) throws Throwable {
        return sftp.get(path);
    }

    @Override
    public void commit() throws Throwable {

    }

    @Override
    public void close() {
        if (channel != null) {
            if (channel.isConnected()) {
                channel.disconnect();
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }

    //    public static void main(String[] args) {
//        List<String> list = listFileNames("192.168.1.189", 22, "merce", "merce", "/app/streaming/runner");
//        System.out.println(list);
//    }
//
//    private static ChannelSftp getFtpsClient(String host, int port, String username, String password) {
//        ChannelSftp sftp = null;
//        Channel channel = null;
//        Session session = null;
//        try {
//            JSch jsch = new JSch();
//            jsch.getSession(username, host, port);
//            session = jsch.getSession(username, host, port);
//            session.setPassword(password);
//            Properties sshConfig = new Properties();
//            sshConfig.put("StrictHostKeyChecking", "no");
//            session.setConfig(sshConfig);
//            session.connect();
//            channel = session.openChannel("sftp");
//            channel.connect();
//            sftp = (ChannelSftp) channel;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return sftp;
//    }
//
//    private static List<String> listFileNames(String host, int port, String username, final String password, String dir) {
//        List<String> list = new ArrayList<String>();
//        ChannelSftp sftp = null;
//        Channel channel = null;
//        Session session = null;
//        try {
//            JSch jsch = new JSch();
//            jsch.getSession(username, host, port);
//            session = jsch.getSession(username, host, port);
//            session.setPassword(password);
//            Properties sshConfig = new Properties();
//            sshConfig.put("StrictHostKeyChecking", "no");
//            session.setConfig(sshConfig);
//            session.connect();
//            channel = session.openChannel("sftp");
//            channel.connect();
//            sftp = (ChannelSftp) channel;
//            Vector<?> vector = sftp.ls(dir);
//            for (Object item : vector) {
//                LsEntry entry = (LsEntry) item;
//                System.out.println(entry.getFilename());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            closeChannel(sftp);
//            closeChannel(channel);
//            closeSession(session);
//        }
//        return list;
//    }
//
//    private static void closeChannel(Channel channel) {
//        if (channel != null) {
//            if (channel.isConnected()) {
//                channel.disconnect();
//            }
//        }
//    }
//
//    private static void closeSession(Session session) {
//        if (session != null) {
//            if (session.isConnected()) {
//                session.disconnect();
//            }
//        }
//    }

    public static void main(String[] args) {
        FtpsClient client = new FtpsClient();
        client.connect("192.168.1.189", 22, "merce", "merce");
        List<FtpFile> files = new ArrayList();
        client.list(files, "/app/streaming/runner", ".*jar", true);
        client.close();
        System.out.println(files);
    }
}