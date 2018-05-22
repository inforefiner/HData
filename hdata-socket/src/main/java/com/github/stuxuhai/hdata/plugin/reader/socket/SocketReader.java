package com.github.stuxuhai.hdata.plugin.reader.socket;

import com.github.stuxuhai.hdata.api.*;
import com.github.stuxuhai.hdata.plugin.reader.socket.netty.Receiver;
import com.github.stuxuhai.hdata.plugin.reader.socket.utils.JsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketReader extends Reader {

    private static final Logger LOGGER = LogManager.getLogger(SocketReader.class);
    private static final String ALL_MATCH_COLUMN = "_match_all_";
    private static final String KEY_MATCH_COLUMN = "_match_key_";

    private String[] extract_columns;
    private String ip_bind;
    private int receiver_port;
    private String null_string;
    private Receiver server;
    private int operate_type;
    private Pattern operate_pattern;
    private Map<String, Pattern> patternMap = new HashMap<>();
    private Map defaultsMap = new HashMap();

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        this.ip_bind = readerConfig.getString(SocketReaderProperties.RECEIVER_BIND, "127.0.0.1");
        this.receiver_port = readerConfig.getInt(SocketReaderProperties.RECEIVER_PORT, 10000);
        this.null_string = readerConfig.getString(SocketReaderProperties.NULL_STRING, "");
        this.operate_type = readerConfig.getInt(SocketReaderProperties.OPERATE_TYPE, 0);
        String columns = readerConfig.getString(SocketReaderProperties.EXTRACT_COLUMNS);
        String operate_regex = readerConfig.getString(SocketReaderProperties.OPERATE_REGEX);
        String defaults_map = readerConfig.getString(SocketReaderProperties.DEFAULTS_MAP);

        try {
            if (StringUtils.isNotBlank(operate_regex)) {
                if(operate_type == 4){
                    Map<String, String> map = JsonBuilder.getInstance().fromJson(operate_regex, HashMap.class);
                    LOGGER.info("operate_regex_mapping json = {}", map);
                    for(String k: map.keySet()){
                        String v = map.get(k);
                        Pattern p = Pattern.compile(v);
                        this.patternMap.put(k, p);
                    }
                }else{
                    this.operate_pattern = Pattern.compile(operate_regex);
                }
                this.extract_columns = columns.split(",");
            }
        } catch (Exception e) {
            LOGGER.error("pattern parse error", e);
            e.printStackTrace();
        }

        try{
            if (StringUtils.isNotBlank(defaults_map)){
                defaultsMap = JsonBuilder.getInstance().fromJson(defaults_map, HashMap.class);
                LOGGER.info("defaults map json = {}", defaultsMap);
            }
        }catch (Exception e) {
            LOGGER.error("defaults mapping parse error", e);
            e.printStackTrace();
        }

        LOGGER.info("SyslogReader prepare finish, receiver port = {}, operate type = {}, operate regex = {}, extract columns = {}, null string = {}", receiver_port, operate_type, operate_regex, columns, null_string);
    }

    @Override
    public void execute(final RecordCollector recordCollector) {
        server = new Receiver(ip_bind, receiver_port, new ReadListener() {
            @Override
            public void callback(String line) {
                if (!line.isEmpty()) {
                    Matcher matcher = null;
                    Record record = null;
                    switch (operate_type) {
                        case 1: //抽取
                            matcher = operate_pattern.matcher(line);
                            if (matcher.find()) {
                                record = new DefaultRecord(extract_columns.length);
                                for (String column : extract_columns) {
                                    if (column.equalsIgnoreCase(ALL_MATCH_COLUMN)) {
                                        record.add(line);
                                    } else {
                                        String str = null;
                                        try {
                                            str = matcher.group(column);
                                        } catch (Exception e) {
                                            //e.printStackTrace();
                                        }
                                        if (str == null) {
                                            if(defaultsMap != null && defaultsMap.containsKey(column)){
                                                str = (String)defaultsMap.get(column);
                                            }else {
                                                str = null_string;
                                            }
                                        }
                                        record.add(str);
                                    }

                                }
                            }
                            break;
                        case 2: //分割
                            String[] arr = operate_pattern.split(line);
                            record = new DefaultRecord(arr.length);
                            for (String str : arr) {
                                record.add(str);
                            }
                            break;
                        case 3: //过滤
                            matcher = operate_pattern.matcher(line);
                            if (matcher.find()) {
                                record = new DefaultRecord(1);
                                record.add(line);
                            }
                            break;
                        case 4: //多表达式抽取
                            for(String k: patternMap.keySet()){
                                Pattern p = patternMap.get(k);
                                Map _defaultsMap = (Map)defaultsMap.get(k);
                                matcher = p.matcher(line);
                                if(matcher.find()){
                                    record = new DefaultRecord(extract_columns.length);
                                    for (String column : extract_columns) {
                                        if (column.equalsIgnoreCase(ALL_MATCH_COLUMN)) {
                                            record.add(line);
                                        } else if (column.equalsIgnoreCase(KEY_MATCH_COLUMN)){
                                            record.add(k);
                                        }else {
                                            String str = null;
                                            try {
                                                str = matcher.group(column);
                                            } catch (Exception e) {
                                                //e.printStackTrace();
                                            }
                                            if (str == null) {
                                                if(_defaultsMap != null && _defaultsMap.containsKey(column)){
                                                    str = (String)_defaultsMap.get(column);
                                                }else {
                                                    str = null_string;
                                                }
                                            }
                                            record.add(str);
                                        }

                                    }
                                    break;
                                }
                            }
                            break;
                        default: //原样
                            record = new DefaultRecord(1);
                            record.add(line);
                    }
                    if (record != null) {
                        recordCollector.send(record);
                    }
                }
            }
        });
        server.start();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public Splitter newSplitter() {
        return null;
    }

}
