package com.github.stuxuhai.hdata.plugin.reader.ftp;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;

public class ChunkedInputStreamBuilder {

    private File target;

    private volatile int countStream;

    private long chunkSize;

    private int totalChunk;

    private long totalSize;

    private String fileName;

    public ChunkedInputStreamBuilder(File file, long chunkSize) {
        this.target = file;
        this.chunkSize = chunkSize;
        this.totalChunk = (int) Math.ceil((double) file.length() / chunkSize);
        this.totalSize = file.length();
        this.fileName = file.getName();
    }

    public synchronized ChunkedInputStream next() {
        if (this.countStream > totalChunk - 1) return null;
        ChunkedInputStream segment = createInputStream();
        this.countStream++;
        return segment;
    }

    protected ChunkedInputStream createInputStream() {
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(target, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ChunkedInputStream ret = null;
        try {
            long pos = countStream * chunkSize;
            long readable = chunkSize;
            if (totalSize - pos < chunkSize) {
                readable = totalSize - pos;
            }
            BoundedInputStream bis = new BoundedInputStream(
                    Channels.newInputStream(randomAccessFile.getChannel().position(pos)), readable);
            bis.setPropagateClose(false);
            ret = new ChunkedInputStream(readable, countStream + 1, bis);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }


    public int getTotalChunk() {
        return totalChunk;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public int getCountStream() {
        return countStream;
    }

    public String getFileName() {
        return fileName;
    }

    public synchronized boolean hasNext() {
        return countStream < totalChunk;
    }

    public static void main(String[] args) {

        long a = 10;
        long b = 3;

//        double c = (double)a / (double)b;

        int d = (int) Math.ceil((double) a / b);

        System.out.println(d);
    }
}