package com.zyc.magic_mirror.common.plugin;

/**
 * 插件返回结果
 */
public interface PluginResult {
    public int getCode();

    public Object getResult();

    public String getMessage();
}
