package com.zyc.plugin;

import com.zyc.common.entity.PluginInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface PluginService {
    public String execute(PluginInfo pluginInfo, List<Map> params, String rs);
    public String execute(PluginInfo pluginInfo, List<Map> params, Set<String> rs);
}
