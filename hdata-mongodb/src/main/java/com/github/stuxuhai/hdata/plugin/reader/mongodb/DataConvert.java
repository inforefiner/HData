package com.github.stuxuhai.hdata.plugin.reader.mongodb;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DataConvert {

    //    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static DataConvert dataConvert;

    public static DataConvert getInstance() {
        if (dataConvert == null) {
            dataConvert = new DataConvert();
        }
        return dataConvert;
    }

    public String convert(Object data) {
        if (data instanceof Integer || data instanceof Double || data instanceof Boolean || data instanceof String || data instanceof Long) {
            return data.toString();
        } else if (data instanceof Date) {
            return timeFormat.format((Date) data);
        } else if (data instanceof BsonTimestamp) {
            BsonTimestamp timestamp = (BsonTimestamp) data;
            return timeFormat.format(new Date(timestamp.getTime() * 1000l));
        } else if (data instanceof ArrayList) {
            ArrayList list = (ArrayList) data;
            List<String> ret = new ArrayList();
            for (Object obj : list) {
                ret.add(convert(obj));
            }
            return StringUtils.join(ret, ",");
        } else if (data instanceof Document) {
            Document doc = (Document) data;
            return doc.toJson();
        }
        if (data != null) {
            return data.toString();
        }
        return null;
    }
}