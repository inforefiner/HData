package com.github.stuxuhai.hdata.ftp;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class HdfsUtil {
    private final Logger logger = LoggerFactory.getLogger(HdfsUtil.class);

    public static HdfsUtil getInstance(String path, String descDir) {
        return new HdfsUtil(path, descDir);
    }

    private FileSystem fs;

    private HdfsUtil(String path, String descDir) {

        try {
            unZipFiles(new File(path), descDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Configuration configuration = new Configuration(false);
            if (StringUtils.isNotBlank(path)) {
                ArrayList<File> fileList = new ArrayList<>();
                getFiles(descDir, fileList);
                for (File file : fileList) {
                    if (file.getName().endsWith(".xml")) {
                        InputStream in = new FileInputStream(file);
                        configuration.addResource(in);
                        logger.info("add resource:" + file.getName());
                    }
                }
//                logger.info("hadoop conf path is:" + path);
//                try (ZipFile zipFile = new ZipFile(path)) {
//                    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path));
//                    ZipInputStream zis = new ZipInputStream(bis);
//
//                    ZipEntry zipEntry;
//                    while ((zipEntry = zis.getNextEntry()) != null) {
//                        if (zipEntry.getName().endsWith(".xml")) {
//                            configuration.addResource(zipFile.getInputStream(zipEntry), zipEntry.getName());
//                            logger.info("add resource:" + zipEntry.getName());
//                        }
//                    }
//                    zis.closeEntry();
//                    bis.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
            } else {
                String CONF_PATH = System.getenv("HADOOP_CONF_DIR");
                if (StringUtils.isNotBlank(CONF_PATH)) {
                    configuration.addResource(new Path(CONF_PATH + File.separator + "core-site.xml"));
                    configuration.addResource(new Path(CONF_PATH + File.separator + "hdfs-site.xml"));
                }
            }
            logger.info("fs site:" +configuration.get("fs.defaultFS"));
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

    public void unZipFiles(File zipFile, String descDir) throws IOException {
        File pathFile = new File(descDir);
        if (!pathFile.exists()) {
            pathFile.mkdirs();
        }
        //解决zip文件中有中文目录或者中文文件
        ZipFile zip = new ZipFile(zipFile);
        for (Enumeration entries = zip.entries(); entries.hasMoreElements(); ) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            String zipEntryName = entry.getName();
            InputStream in = zip.getInputStream(entry);
            String outPath = (descDir + zipEntryName).replaceAll("\\*", "/");
            ;
            //判断路径是否存在,不存在则创建文件路径
            File file = new File(outPath.substring(0, outPath.lastIndexOf('/')));
            if (!file.exists()) {
                file.mkdirs();
            }
            //判断文件全路径是否为文件夹,如果是上面已经上传,不需要解压
            if (new File(outPath).isDirectory()) {
                continue;
            }
            //输出文件路径信息
            logger.info("unzip file:" + outPath);
            OutputStream out = new FileOutputStream(outPath);
            byte[] buf1 = new byte[1024];
            int len;
            while ((len = in.read(buf1)) > 0) {
                out.write(buf1, 0, len);
            }
            in.close();
            out.close();
        }
        logger.info("unzip successfully");
    }

    public void getFiles(String path, ArrayList<File> list) {
        //目标集合fileList
        File file = new File(path);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File fileIndex : files) {
                //如果这个文件是目录，则进行递归搜索
                if (fileIndex.isDirectory()) {
                    getFiles(fileIndex.getPath(), list);
                } else {
                    //如果文件是普通文件，则将文件句柄放入集合中
                    list.add(fileIndex);
                }
            }
        }
    }



}
