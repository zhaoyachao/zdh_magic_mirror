package com.zyc.common.plugin;

import com.zyc.common.entity.PluginInfo;

/**
 * 统一插件接口
 */
public interface PluginService {

    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, String rs);

    public PluginParam getPluginParam(Object param);
}
