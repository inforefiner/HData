package com.github.stuxuhai.hdata.plugin.reader.http.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * created by pengbinxuan on 2019/8/15 0015
 */
public class DateUtils {

    /**
     * Gets the value of the corresponding schema
     *
     * @param String newDataValue
     * @return String typeDataValue
     */
    public String dateFromat(String format, Date date) {
        SimpleDateFormat allFormat = new SimpleDateFormat(format);
        String typeDataValue = allFormat.format(date);
        return typeDataValue;
    }

    /**
     * get DateUtils
     *
     * @return DateUtils
     */
    public static DateUtils getInstance() {
        return new DateUtils();
    }

    public static void main(String[] args) {
        System.out.println("dateFromat:  " + DateUtils.getInstance().dateFromat("yyyy-MM-dd HH:mm:ss", new Date()));
    }
}