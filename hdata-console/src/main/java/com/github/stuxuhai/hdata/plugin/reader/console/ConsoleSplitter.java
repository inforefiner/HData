package com.github.stuxuhai.hdata.plugin.reader.console;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;

import java.util.ArrayList;
import java.util.List;

public class ConsoleSplitter extends Splitter {


    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String cmds = readerConfig.getString(ConsoleReaderProperties.EXEC_COMMAND_LIST);
        if (cmds != null && !cmds.isEmpty()) {
            for (String cmd : cmds.split(",")) {
                PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                pluginConfig.put(ConsoleReaderProperties.EXEC_COMMAND, cmd);
                list.add(pluginConfig);
            }
        }
        return list;
    }
}
