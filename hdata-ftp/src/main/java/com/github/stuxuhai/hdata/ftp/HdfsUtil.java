package com.github.stuxuhai.hdata.ftp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class HdfsUtil {

    private static HdfsUtil hdfsUtil = new HdfsUtil();

    public static HdfsUtil getInstance() {
        return hdfsUtil;
    }

    private FileSystem fs;

    private HdfsUtil() {
        try {
            Configuration configuration = new Configuration();
            String CONF_PATH = System.getenv("HADOOP_CONF_DIR");
            if (StringUtils.isNotBlank(CONF_PATH)) {
                configuration.addResource(new Path(CONF_PATH + File.separator + "core-site.xml"));
                configuration.addResource(new Path(CONF_PATH + File.separator + "hdfs-site.xml"));
            }
            this.fs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public OutputStream create(String path) {
        try {
            return fs.create(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("can't create OutputStream on HDFS");
    }

    public boolean rename(String oldPath, String newPath) {
        try {
            return fs.rename(new Path(oldPath), new Path(newPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("can't create OutputStream on HDFS");
    }

    public boolean exist(String path) {
        try {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public FileStatus status(String path) {
        if (!exist(path)) return null;
        try {
            FileStatus status = fs.getFileStatus(new Path(path));
            return status;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void delete(String path) {
        if (exist(path)) {
            try {
                fs.delete(new Path(path), false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
