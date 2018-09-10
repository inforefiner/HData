package com.github.stuxuhai.hdata.plugin.reader.ftp;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSender {

    private static long chunkSize = 1024 * 1024 * 5l;

    private String postUrl;

    private String dstPath;

    private ChunkedInputStreamBuilder builder;

    private ExecutorService executorService;

    public FileSender(String postUrl, String filePath, String dstPath) {
        this.postUrl = postUrl;
        this.dstPath = dstPath;
        File target = new File(filePath);
        builder = new ChunkedInputStreamBuilder(target, chunkSize);
    }

    public boolean exists(long fileSize, long modificationTime) {
        boolean ret = false;
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(postUrl + "/exists?dstPath=" + dstPath + "&fileSize=" + fileSize + "&modificationTime=" + modificationTime);
        HttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(httpGet);
            if (httpResponse.getStatusLine().getStatusCode() == 304) {
                ret = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public void send() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        String identifier = UUID.randomUUID().toString();
        List<FileSenderThread> tasks = new ArrayList();
        while (builder.hasNext()) {
            ChunkedInputStream cis = builder.next();
            List<NameValuePair> params = new ArrayList();
            params.add(new BasicNameValuePair("chunkSize", "" + chunkSize));
            params.add(new BasicNameValuePair("totalChunks", "" + builder.getTotalChunk()));
            params.add(new BasicNameValuePair("totalSize", "" + builder.getTotalSize()));
            params.add(new BasicNameValuePair("identifier", identifier));
            params.add(new BasicNameValuePair("filename", builder.getFileName()));
            params.add(new BasicNameValuePair("relativePath", builder.getFileName()));
            params.add(new BasicNameValuePair("dstPath", dstPath));
            params.add(new BasicNameValuePair("resourceId", ""));
            params.add(new BasicNameValuePair("chunkNumber", "" + cis.getIndex()));
            tasks.add(new FileSenderThread(cis.getInputStream(), cis.getReadable(), params, postUrl));
        }
        try {
            executorService.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}

class FileSenderThread implements Callable<Boolean> {
    BoundedInputStream bis;
    long readable;
    String postUrl;
    List<NameValuePair> params;

    public FileSenderThread(BoundedInputStream bis, long readable, List<NameValuePair> params, String postUrl) {
        this.bis = bis;
        this.readable = readable;
        this.params = params;
        this.postUrl = postUrl;
    }

    @Override
    public Boolean call() throws Exception {
        boolean ret = false;
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(postUrl + "?" + URLEncodedUtils.format(params, Charset.defaultCharset()));
        try {
            HttpEntity httpEntity = new InputStreamEntity(bis, readable, ContentType.DEFAULT_BINARY);
            post.setEntity(httpEntity);
            HttpResponse httpResponse = httpClient.execute(post);
            if (httpResponse.getStatusLine().getStatusCode() == 200) {
                ret = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ret;
    }
}