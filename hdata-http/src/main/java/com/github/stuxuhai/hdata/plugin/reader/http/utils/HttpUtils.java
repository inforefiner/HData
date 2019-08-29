package com.github.stuxuhai.hdata.plugin.reader.http.utils;

import org.apache.commons.lang3.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by pengbinxuan on 2019/8/15 0015
 */
public class HttpUtils {

    private static final String DATA_PERFIX = "${date:";
    private static final String DATA_SUFFIX = "}";
    private static final String SPLIT_SING = "&";
    private static final String KV_SING = "=";
    private static final String DATE_SEMICOLON = "date:";

    /**
     * @param String url
     * @return String newUrl
     */
    public String discernUrl(String url) {
        if (StringUtils.isNotBlank(url) && url.contains(DATA_PERFIX) && url.contains(DATA_SUFFIX)) {
            String newUrl = "";
            try {
                URL u = new URL(url);
                newUrl = changeUrl(u);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            return newUrl;
        } else
            return url;
    }

    /**
     * The URL is parsed
     *
     * @param URL u
     * @return String changeUrl
     */
    public String changeUrl(URL url) {
        //type=opt&date=${date:yyyyMMdd}&date2=${date:yyyyMMdd HH:mm:ss}
        String option = url.getQuery();
        String correctUrl = url.toString().split("\\?")[0];
        DateUtils dateUtils = DateUtils.getInstance();
        String result = "";
        AtomicReference<String> newQuery = new AtomicReference<>("");
        if (StringUtils.isNotBlank(option)) {
            Arrays.stream(option.trim().split(SPLIT_SING)).forEach(
                    (kv) -> {
                        Arrays.stream(kv.split(KV_SING)).forEach(
                                (rs) -> {
                                    if (rs.contains(DATA_PERFIX) && rs.contains(DATA_SUFFIX)) {
                                        rs = dateUtils.dateFromat(changeUrlOption(rs), new Date());
                                    }
                                    newQuery.set(newQuery + rs + KV_SING);
                                }
                        );
                        newQuery.set(newQuery + SPLIT_SING);
                    }
            );
            result = newQuery.toString().substring(0, newQuery.toString().length() - 2);
        }
        result = correctUrl + "?" + result;
        System.out.println("result: " + result);
        return result;
    }

    public String changeUrlOption(String result) {
        return result.split(DATE_SEMICOLON)[1].trim().split(DATA_SUFFIX)[0];
    }

    public static HttpUtils getInstance() {
        return new HttpUtils();
    }

    public static void main(String[] args) {
//        getInstance().changeUrlOption("${date:yyyyMMdd}");
        String url = "http://localhost:8080/list?type=opt&date=${date:yyyy-MM-dd'T'HH:mm:ss'Z'}&date2=${date:yyyyMMdd}";
        HttpUtils.getInstance().discernUrl(url);
    }
}