package com.github.stuxuhai.hdata.plugin.reader.mongodb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {


    public static void main(String[] args) {

        Date d = new Date();

        System.out.println("d = " + d);

        String str = d.toString();

        System.out.println("str = " + str);

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
//            Date dd = DateFormat.getDateInstance().parse(str);
            System.out.println("dd = " + sdf.parse(str));
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }
}
