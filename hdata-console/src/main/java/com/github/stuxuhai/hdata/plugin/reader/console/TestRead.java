package com.github.stuxuhai.hdata.plugin.reader.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRead {


    public static void main(String[] args) {


        Pattern regex = Pattern.compile("(?<time>(^[\\d/]+\\s+[\\d:]+))\\s(?<level>(\\w+))\\s(?<message>(.+))");

        String[] envArray = new String[]{};
        String cmd = "cat /Users/joey/tmp/1.txt";

        Process p = null;
        try {
            p = Runtime.getRuntime().exec(cmd, envArray, new File("."));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        try {
            String line;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                Matcher matcher = regex.matcher(line);
                if (matcher.find()) {
//                    System.out.println(line);
                    System.out.println(matcher.group("time") + " - " + matcher.group("level") + " - " +matcher.group("message"));
                }
            }
            br.close();
        } catch (IOException e) {
        }
    }
}
