package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.PluginInfo;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.plugin.dao.PluginMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class PluginServiceImpl {

    public PluginInfo selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            PluginMapper pluginMapper = sqlSession.getMapper(PluginMapper.class);
            return pluginMapper.selectOne(id);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
