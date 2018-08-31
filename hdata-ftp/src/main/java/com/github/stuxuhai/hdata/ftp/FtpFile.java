package com.github.stuxuhai.hdata.ftp;

public class FtpFile {

    String path;
    long size;
    long modificationTime;

    public FtpFile(String path, long size, long modificationTime) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }
}
