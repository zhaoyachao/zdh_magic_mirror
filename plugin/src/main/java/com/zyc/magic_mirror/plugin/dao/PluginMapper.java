package com.zyc.magic_mirror.plugin.dao;

import com.zyc.magic_mirror.common.entity.PluginInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.BaseMapper;


public interface PluginMapper extends BaseMapper<PluginInfo> {

    @Select({
            "select * from plugin_info where plugin_code = #{plugin_code} "
    })
    public PluginInfo selectByPluginCode(@Param("plugin_code") String plugin_code);
}
