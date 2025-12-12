package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.PluginInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.plugin.dao.PluginMapper;

public class PluginServiceImpl extends BaseServiceImpl {

    public PluginInfo selectById(String id){

        return executeReadOnly(sqlSession -> {
            PluginMapper pluginMapper = sqlSession.getMapper(PluginMapper.class);
            return pluginMapper.selectByPluginCode(id);
        });
    }
}
