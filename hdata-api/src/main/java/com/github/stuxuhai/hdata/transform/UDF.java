package com.github.stuxuhai.hdata.transform;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

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
        if (obj instanceof String) {
            String str = obj.toString();
            return str.replaceAll(".", "*");
        }
        return "*";
    }

    /**
     * encrypt加密方法
     */
    public Object encrypt(Object obj, String encryptKey) {
        StandardPBEStringEncryptor stringEncryptor = new StandardPBEStringEncryptor();
        stringEncryptor.setAlgorithm("PBEWithMD5AndDES");          // 加密的算法，这个算法是默认的
        stringEncryptor.setPassword(encryptKey); // 加密的密钥,可以自定义,但必须和配置文件中配置的解密密钥一致

        if (obj instanceof String) {
            return stringEncryptor.encrypt(obj.toString());
        } else {
            return obj;
        }
    }

    /**
     * 计算该记录的checksum值
     *
     * @param record
     * @param checksumInx 该下标指向的值不在checksum计算之内
     * @return
     */
    public String checksum(Object[] record, int checksumInx) {
        //_record 剔除checksumInx坐标值，生成新的Object array.
        List<Object> r = new ArrayList<>();
        for (int i = 0; i < record.length; i++) {
            if (i != checksumInx) {
                r.add(record[i]);
            }
        }
        byte[] data = Arrays.toString(r.toArray()).getBytes();
        CRC32 crc = new CRC32();
        crc.update(data);
        return String.valueOf(crc.getValue());
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
