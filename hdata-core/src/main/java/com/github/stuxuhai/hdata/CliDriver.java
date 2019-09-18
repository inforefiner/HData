package com.github.stuxuhai.hdata;

import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.TransformConfig;
import com.github.stuxuhai.hdata.config.DefaultJobConfig;
import com.github.stuxuhai.hdata.core.HData;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Throwables;
import org.apache.commons.cli.*;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLDecoder;
import java.util.Map.Entry;
import java.util.Properties;


public class CliDriver {

    private static final String XML_FILE_OPTION = "f";
    private static final String HDATA_VARS_OPTION = "D";
    private static final String QUIET_OPTION = "q";
    private static final String READER_OPTION = "reader";
    private static final String WRITER_OPTION = "writer";
    private static final String READER_VARS_OPTION = "R";
    private static final String WRITER_VARS_OPTION = "W";
    private static final String TRANSFORMER_VARS_OPTION = "T";
    private static final String ENCRYPT_KEY ="encryptKey";
    private static final String ENCRYPT_COLUMNS="encryptColumns";
    private static final String CHECKSUM_COLUMN = "checksumColumn";

    private static final Logger LOGGER = LoggerFactory.getLogger(CliDriver.class);

    /**
     * 创建命令行选项
     *
     * @return
     */
    public Options createOptions() {
        Options options = new Options();
        options.addOption(XML_FILE_OPTION, null, true, "job xml path");
        options.addOption(QUIET_OPTION, null, false, "quiet");
        options.addOption(Option.builder(HDATA_VARS_OPTION).hasArgs().build());

        options.addOption(null, READER_OPTION, true, "reader name");
        options.addOption(Option.builder(READER_VARS_OPTION).hasArgs().build());

        options.addOption(null, WRITER_OPTION, true, "writer name");
        options.addOption(Option.builder(WRITER_VARS_OPTION).hasArgs().build());

        options.addOption(Option.builder(TRANSFORMER_VARS_OPTION).hasArgs().build());

        options.addOption(null, ENCRYPT_KEY, true,"encrypt key string");
        options.addOption(null, ENCRYPT_COLUMNS, true, "encrypt columns string");
        options.addOption(null, CHECKSUM_COLUMN, true, "checksum column string");
        return options;
    }

    /**
     * 打印命令行帮助信息
     *
     * @param options
     */
    public void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    /**
     * 替换命令行变量
     *
     * @param config
     * @param vars
     */
    public void replaceConfigVars(PluginConfig config, Properties vars) {
        for (Entry<Object, Object> confEntry : config.entrySet()) {
            if (confEntry.getKey().getClass() == String.class && confEntry.getValue().getClass() == String.class) {
                for (Entry<Object, Object> varEntry : vars.entrySet()) {
                    String replaceVar = "${" + varEntry.getKey() + "}";
                    if (confEntry.getValue().toString().contains(replaceVar)) {
                        config.put(confEntry.getKey(), confEntry.getValue().toString().replace(replaceVar, varEntry.getValue().toString()));
                    }
                }
            }
        }
    }

    private void putOptionValues(Properties props, String[] values) {
        if (props != null && values != null) {
            for (int i = 0; i < values.length; i++) {
                String str = values[i];
                try {
                    str = URLDecoder.decode(str, "utf-8");
                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
                }
                int idx = str.indexOf("=");
                String key = str.substring(0, idx);
                String value = str.substring(idx + 1);
                props.put(key, value);
            }
//            for (int i = 0, i < values.length; i++) {
//                String str = values[i];
//                props.put(values[i], values[++i]);
//            }
        }
    }

    private static void HandleArgsByOs(String[] args) {
        if (args != null && SystemUtils.IS_OS_WINDOWS) {
            for (int i = 0; i < args.length; i++) {
                System.out.println("before = " + args[i]);
                args[i] = args[i].replaceAll("#", "=");
                System.out.println("after = " + args[i]);
            }
        }
    }


    /**
     * 主程序入口
     *
     * @param args
     */
    public static void main(String[] args) {

        HandleArgsByOs(args);

        LOGGER.info("ARGS = {}", StringUtils.join(args, ","));

        CliDriver cliDriver = new CliDriver();
        Options options = cliDriver.createOptions();
        LOGGER.info("create options");
        if (args.length < 1) {
            cliDriver.printHelp(options);
            System.exit(-1);
        }
        LOGGER.info("create options successfully");

        CommandLineParser parser = new DefaultParser();

        CommandLine cmd = null;
        try {
            LOGGER.info("parse cmd");
            cmd = parser.parse(options, args);
            LOGGER.info("parse cmd successfully");
            if (cmd.hasOption(QUIET_OPTION)) {
            }

            final DefaultJobConfig jobConfig;
            if (cmd.hasOption(XML_FILE_OPTION)) {
                String jobXmlPath = cmd.getOptionValue(XML_FILE_OPTION);
                jobConfig = DefaultJobConfig.createFromXML(jobXmlPath);
                Properties vars = new Properties();
                cliDriver.putOptionValues(vars, cmd.getOptionValues(HDATA_VARS_OPTION));

                final PluginConfig readerConfig = jobConfig.getReaderConfig();
                final PluginConfig writerConfig = jobConfig.getWriterConfig();

                cliDriver.replaceConfigVars(readerConfig, vars);
                cliDriver.replaceConfigVars(writerConfig, vars);
            } else {
                if (!cmd.hasOption(READER_OPTION) || !cmd.hasOption(WRITER_OPTION)) {
                    throw new HDataException("Option --reader and --writer should be both given if -f option not exists.");
                }

                String readerName = cmd.getOptionValue(READER_OPTION);
                String writerName = cmd.getOptionValue(WRITER_OPTION);

                PluginConfig readerConfig = new PluginConfig();
                cliDriver.putOptionValues(readerConfig, cmd.getOptionValues(READER_VARS_OPTION));

                PluginConfig writerConfig = new PluginConfig();
                cliDriver.putOptionValues(writerConfig, cmd.getOptionValues(WRITER_VARS_OPTION));

                TransformConfig transformConfig = new TransformConfig();
                cliDriver.putOptionValues(transformConfig, cmd.getOptionValues(TRANSFORMER_VARS_OPTION));

                LOGGER.info("add encryptKey: " + cmd.getOptionValue(ENCRYPT_KEY));
                transformConfig.encryptKey = cmd.getOptionValue(ENCRYPT_KEY);
//                String encryptKey = cmd.getOptionValue(ENCRYPT_KEY);
//                String encryptColumns = cmd.getOptionValue(ENCRYPT_COLUMNS);
//                String checksumColumn = cmd.getOptionValue(CHECKSUM_COLUMN);
//                transformConfig.others.put(ENCRYPT_KEY, encryptKey);//新增
//                transformConfig.others.put(ENCRYPT_COLUMNS, encryptColumns);//新增
//                transformConfig.others.put(CHECKSUM_COLUMN, checksumColumn);//新增

                jobConfig = new DefaultJobConfig(readerName, readerConfig, writerName, writerConfig, transformConfig);
                LOGGER.info("init job config");
            }

            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.split("@")[0];
            LOGGER.info("#PID={}#", pid);

            HData hData = new HData();
            hData.start(jobConfig);
        } catch (ParseException e) {
            cliDriver.printHelp(options);
            System.exit(-1);
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            System.exit(-1);
        }
    }
}
