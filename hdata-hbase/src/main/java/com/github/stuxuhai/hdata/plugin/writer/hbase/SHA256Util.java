package com.github.stuxuhai.hdata.plugin.writer.hbase;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA256Util {
    /**
     * 利用java原生的摘要实现SHA256加密
     *
     * @param str 加密后的报文
     * @return
     */
    public static String getSHA256StrJava(String str) {
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes(StandardCharsets.UTF_8));
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return encodeStr;
    }

    /**
     * 将byte转为16进制
     *
     * @param bytes
     * @return
     */
    private static String byte2Hex(byte[] bytes) {
        StringBuffer stringBuffer = new StringBuffer();
        String temp = null;
        for (byte aByte : bytes) {
            temp = Integer.toHexString(aByte & 0xFF);
            if (temp.length() == 1) {
                //1得到一位的进行补0操作
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }

    public static void main(String[] args) {
        //d7ef0a04f3c8055644677299a9414a75adcb15916eb48417416c9317ace2ff4f
        String test = "vf:id";
        System.out.println("==== test : " + getSHA256StrJava(test));

        String a = URLDecoder.decode("%7B%22job%22%3A%7B%22content%22%3A%5B%7B%22reader%22%3A%7B%22name%22%3A%22oraclereader%22%2C%22parameter%22%3A%7B%22username%22%3A%22carpo%22%2C%22password%22%3A%22123456%22%2C%22connection%22%3A%5B%7B%22jdbcUrl%22%3A%5B%22jdbc%3Aoracle%3Athin%3A%40192.168.1.83%3A1521%3Aorcl%22%5D%2C%22table%22%3A%5B%22blob_test%22%5D%7D%5D%2C%22where%22%3A%22%22%2C%22queryTimeOut%22%3A0%2C%22fetchSize%22%3A10%2C%22column%22%3A%5B%7B%22name%22%3A%22blob%22%2C%22type%22%3A%22binary%22%7D%2C%7B%22name%22%3A%22id%22%2C%22type%22%3A%22double%22%7D%5D%7D%7D%2C%22writer%22%3A%7B%22name%22%3A%22hbasewriter%22%2C%22parameter%22%3A%7B%22table%22%3A%22default%3Ablob_test%22%2C%22hbaseConfig%22%3A%7B%22hbase.zookeeper.quorum%22%3A%22192.168.1.81%2C192.168.1.83%2C192.168.1.83%22%2C%22hbase.zookeeper.property.clientPort%22%3A%222181%22%2C%22zookeeper.znode.parent%22%3A%22%2Fhbase-unsecure%22%7D%2C%22rowkeyColumn%22%3A%22SHA256%28%24%28kf%3Ablob%29%29%22%2C%22column%22%3A%5B%7B%22name%22%3A%22kf%3Ablob%22%2C%22type%22%3A%22binary%22%7D%2C%7B%22name%22%3A%22vf%3Aid%22%2C%22type%22%3A%22double%22%7D%5D%7D%7D%7D%5D%2C%22setting%22%3A%7B%22restore%22%3A%7B%22isRestore%22%3Afalse%2C%22isStream%22%3Afalse%2C%22maxRowNumForCheckpoint%22%3A1%7D%2C%22log%22%3A%7B%22isLogger%22%3Afalse%2C%22level%22%3A%22info%22%2C%22path%22%3A%22%2Ftmp%2Fdtstack%2Fflinkx%2F%22%7D%2C%22speed%22%3A%7B%22channel%22%3A1%2C%22bytes%22%3A9223372036854775807%7D%2C%22errorLimit%22%3A%7B%22record%22%3A0%2C%22percentage%22%3A0.0%7D%7D%7D%7D");
        System.out.println(a);
    }
}
