package com.github.stuxuhai.hdata.plugin.reader.console;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.exception.HDataException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsoleReader extends Reader {

    private BufferedReader br = null;

    private Pattern extract_regex;
    private String[] extract_columns;

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        String[] envArray = new String[]{};
        String env = readerConfig.getString(ConsoleReaderProperties.EXEC_ENV, "");
        if(!env.isEmpty()) {
            envArray = env.split(",");
        }
        String cmd = readerConfig.getString(ConsoleReaderProperties.EXEC_COMMAND);
        String path = readerConfig.getString(ConsoleReaderProperties.EXEC_PATH, ".");
        Process p = null;
        try {
            p = Runtime.getRuntime().exec(cmd, envArray, new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        br = new BufferedReader(new InputStreamReader(p.getInputStream()));

        String columns = readerConfig.getString(ConsoleReaderProperties.EXTRACT_COLUMNS);
        String regex = readerConfig.getString(ConsoleReaderProperties.EXTRACT_REGEX);

        extract_regex = Pattern.compile(regex);
        extract_columns = columns.split(",");
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        try {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = extract_regex.matcher(line);
                if (matcher.find()) {
                    Record record = new DefaultRecord(extract_columns.length);
                    for (String column : extract_columns) {
                        String str = null;
                        try {
                            str = matcher.group(column);
                        } catch (Exception e) {
                            //e.printStackTrace();
                            str = "";
                        }
                        record.add(str);
                    }
                    recordCollector.send(record);
                }
            }
            br.close();
        } catch (IOException e) {
            new HDataException(e);
        }
    }

    @Override
    public Splitter newSplitter() {
        return new ConsoleSplitter();
    }

}
