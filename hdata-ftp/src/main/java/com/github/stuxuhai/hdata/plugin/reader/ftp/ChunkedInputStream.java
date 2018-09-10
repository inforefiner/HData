package com.github.stuxuhai.hdata.plugin.reader.ftp;

import org.apache.commons.io.input.BoundedInputStream;

public class ChunkedInputStream {

    private long readable;

    private int index;

    private BoundedInputStream inputStream;

    public ChunkedInputStream(long readable, int index, BoundedInputStream inputStream) {
        this.readable = readable;
        this.index = index;
        this.inputStream = inputStream;
    }

    public long getReadable() {
        return readable;
    }

    public void setReadable(long readable) {
        this.readable = readable;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public BoundedInputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(BoundedInputStream inputStream) {
        this.inputStream = inputStream;
    }
}