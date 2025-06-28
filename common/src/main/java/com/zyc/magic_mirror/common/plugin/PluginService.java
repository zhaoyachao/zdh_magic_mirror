package com.zyc.magic_mirror.common.plugin;

import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.PluginInfo;

import java.util.Map;
import java.util.Set;

/**
 * 统一插件接口
 */
public interface PluginService {

    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, DataPipe rs, Map<String,Object> params);

    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, Set<DataPipe> rs, Map<String,Object> params);

    public PluginParam getPluginParam(Object param);
}
