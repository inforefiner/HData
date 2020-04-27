package com.github.stuxuhai.hdata.ftp;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class HdfsUtil {

    public static HdfsUtil getInstance(String path) {
        return new HdfsUtil(path);
    }

    private FileSystem fs;

    private HdfsUtil(String path) {
        try {
            Configuration configuration = new Configuration();
            if (StringUtils.isNotBlank(path)) {
                try (ZipFile zipFile = new ZipFile(path)) {
                    Enumeration<? extends ZipEntry> zipEntryEnumeration = zipFile.entries();
                    if (zipEntryEnumeration.hasMoreElements()) {
                        ZipEntry zipEntry = zipEntryEnumeration.nextElement();
                        if (zipEntry.getName().endsWith(".xml")) {
                            configuration.addResource(zipFile.getInputStream(zipEntry), zipEntry.getName());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    File file = new File(path);
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> file.delete()));
                }

            } else {
                String CONF_PATH = System.getenv("HADOOP_CONF_DIR");
                if (StringUtils.isNotBlank(CONF_PATH)) {
                    configuration.addResource(new Path(CONF_PATH + File.separator + "core-site.xml"));
                    configuration.addResource(new Path(CONF_PATH + File.separator + "hdfs-site.xml"));
                }
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
