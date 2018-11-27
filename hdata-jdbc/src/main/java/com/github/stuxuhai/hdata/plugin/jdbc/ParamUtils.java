package com.github.stuxuhai.hdata.plugin.jdbc;

public class ParamUtils {

    public static int getInt(String key, int def) {
        Object val = get(key);
        if (val == null) {
            return def;
        } else {
            try {
                return Integer.valueOf(val.toString());
            } catch (NumberFormatException e) {
                return def;
            }
        }
    }

    public static long getLong(String key, long def) {
        Object val = get(key);
        if (val == null) {
            return def;
        } else {
            try {
                return Long.valueOf(val.toString());
            } catch (NumberFormatException e) {
                return def;
            }
        }
    }

    private static Object get(String key) {
        return System.getProperty(key);
    }


    public static void main(String[] args) {


        System.out.println(getLong("test.long", 0l));
        System.out.println(getLong("test.int", 0));
    }
}
