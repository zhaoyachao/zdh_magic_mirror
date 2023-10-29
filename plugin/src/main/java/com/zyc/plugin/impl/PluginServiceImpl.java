package com.zyc.plugin.impl;


import com.zyc.common.entity.PluginInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.plugin.dao.PluginMapper;
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
            if(sqlSession != null)
                sqlSession.close();
        }

        return null;
    }
}
