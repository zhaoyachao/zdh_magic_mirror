package com.zyc.plugin.dao;

import com.zyc.common.entity.PluginInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface PluginMapper {

    @Select({
            "select * from plugin_info where plugin_code = #{plugin_code} "
    })
    public PluginInfo selectOne(@Param("plugin_code") String plugin_code);
}
