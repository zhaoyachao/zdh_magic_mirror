package com.zyc.common.plugin;

import com.zyc.common.entity.DataPipe;
import com.zyc.common.entity.PluginInfo;

import java.util.Map;

/**
 * 统一插件接口
 */
public interface PluginService {

    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, DataPipe rs, Map<String,Object> params);

    public PluginParam getPluginParam(Object param);
}
