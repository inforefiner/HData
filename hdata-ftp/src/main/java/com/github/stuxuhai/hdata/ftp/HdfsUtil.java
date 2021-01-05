package com.github.stuxuhai.hdata.ftp;

import com.github.stuxuhai.hdata.config.DefaultEngineConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class HdfsUtil {
    private final Logger log = LoggerFactory.getLogger(HdfsUtil.class);

    private static HdfsUtil hdfsUtil = new HdfsUtil();

    public static HdfsUtil getInstance() {
        return hdfsUtil;
    }

    private FileSystem fs;

    private HdfsUtil() {
        DefaultEngineConfig engineConfig = DefaultEngineConfig.create();
        boolean kerberos = engineConfig.getBoolean("kerberos.enabled", false);
        String kerberosKrb5 = engineConfig.getString("kerberos.krb5");
        String kerberosPrincipal = engineConfig.getString("kerberos.principal");
        String kerberosKeytab = engineConfig.getString("kerberos.keytab");
        log.info("kerberos: " + kerberos);
        log.info("kerberosKrb5: " + kerberosKrb5);
        log.info("kerberosPrincipal: " + kerberosPrincipal);
        log.info("kerberosKeytab: " + kerberosKeytab);

        try {
            Configuration configuration = new Configuration();

            if (kerberos) {
                System.setProperty("java.security.krb5.conf", kerberosKrb5);
                configuration.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(configuration);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

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
