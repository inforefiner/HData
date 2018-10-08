package com.inforefiner.hdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtil {

    private FileSystem fs;

    public HdfsUtil(Configuration config) {
        try {
            this.fs = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mkdirs(String... targetPaths) throws Exception {
        for (String p : targetPaths)
            fs.mkdirs(new Path(p));
    }

    public void copy(String src, String dst) throws Exception {
        fs.copyToLocalFile(new Path(src), new Path(dst));
    }

//    public void copy(String dstPath, String... srcPaths) throws Exception {
//        for (String src : srcPaths) {
//            if (exist(src)) {
//                fs.copyToLocalFile(new Path(src), new Path(dstPath));
//            }
//        }
//    }

    public void upload(String filePath, String targetPath) throws Exception {
        fs.copyFromLocalFile(false, true, new Path(filePath), new Path(targetPath));
    }

    public boolean exist(String path) throws Exception {
        return fs.exists(new Path(path));
    }

    public FileStatus status(String path) throws Exception {
        if (exist(path)) {
            return fs.getFileStatus(new Path(path));
        }
        return null;
    }

    public List<FileStatus> list(String... paths) throws Exception {
        List<FileStatus> ret = new ArrayList();
        for (String path : paths) {
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(path), true);
            while (iterator.hasNext()) {
                LocatedFileStatus status = iterator.next();
                ret.add(status);
            }
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String hadoop_home = "/Users/joey/tmp/hadoop1";
        System.setProperty("HADOOP_CONF_DIR", hadoop_home);
        conf.addResource(new Path(hadoop_home, "core-site.xml"));
        conf.addResource(new Path(hadoop_home, "hdfs-site.xml"));
        conf.addResource(new Path(hadoop_home, "yarn-site.xml"));

        String APP_MASTER_JAR_NAME = "AppMaster.jar";
        Path APP_MASTER_IN_HDFS = new Path("tmp", APP_MASTER_JAR_NAME);

        System.out.println("isAbsolute = " + APP_MASTER_IN_HDFS.isAbsolute());

        URL fileUrl = ConverterUtils.getYarnUrlFromPath(
                FileContext.getFileContext().makeQualified(APP_MASTER_IN_HDFS));


        System.out.println(fileUrl);

//        HdfsUtil hdfsUtil = new HdfsUtil(conf);
//        hdfsUtil.upload();
//        FileStatus fileStatus = hdfsUtil.status(APP_MASTER_IN_HDFS);
//        String path = fileStatus.getPath().toString();
//        System.out.println(path);
//        int idx = path.indexOf("/", 7);
//        System.out.println(path.substring(idx));
//        System.out.println(fileStatus.getPath().toString());
//        System.setProperty("HADOOP_CONF_DIR", "/Users/joey/tmp/hadoop");
//        Path path = new Path("/tmp/athenax/libs/activation-1.1.jar");
//        FileSystem fs = FileSystem.get(loadConf());
//        System.out.println(fs.getFileStatus(path).getPath().toUri());
//        System.out.println(fs.getUri());
//        System.out.println(path.);
//        try {
//            boolean ret =  getInstance().clean("/tmp/wyk_test");
//            System.out.println(ret);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
