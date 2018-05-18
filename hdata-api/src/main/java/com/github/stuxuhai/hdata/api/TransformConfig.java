package com.github.stuxuhai.hdata.api;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by joey on 2017/7/5.
 */
public class TransformConfig extends Configuration {


    public TransformConfig() {
        super();
    }

    public Map<Integer, String> getUdfMap() {
        Map<Integer, String> udfMap = new HashMap();
        Enumeration enumeration = propertyNames();
        while (enumeration.hasMoreElements()) {
            String key = (String) enumeration.nextElement();
            String value = getString(key);
            udfMap.put(Integer.valueOf(key), value);
        }
        return udfMap;
    }
}
