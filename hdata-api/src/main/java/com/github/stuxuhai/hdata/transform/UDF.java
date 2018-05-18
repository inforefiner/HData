package com.github.stuxuhai.hdata.transform;

/**
 * Created by joey on 2017/7/5.
 */
public class UDF {

    public Object blank(Object obj) {
        if (obj instanceof String) {
            return "";
        }
        if (obj instanceof Number) {
            return 0;
        }
        return obj;
    }

    public Object mix(Object obj) {
        if (obj != null && obj instanceof String) {
            String str = obj.toString();
            return str.replaceAll(".", "*");
        }
        return "*";
    }


    public static boolean isNumber(Object obj) {
        return obj instanceof Number;
    }

    public static void main(String[] args) {
        double d = 1123.1234234d;
        float f = 2.2234234f;
        System.out.println(isNumber(d));
        System.out.println(isNumber(f));
        System.out.println(isNumber(null));
    }
}
