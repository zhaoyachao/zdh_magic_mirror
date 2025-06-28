package com.zyc.magic_mirror.common.plugin;

import com.zyc.magic_mirror.common.entity.DataPipe;

import java.util.Set;

/**
 * 插件返回结果
 */
public interface PluginResult {
    public int getCode();

    public Object getResult();

    public Set<DataPipe> getBatchResult();

    public String getMessage();
}
