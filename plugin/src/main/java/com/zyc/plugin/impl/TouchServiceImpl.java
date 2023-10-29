package com.zyc.plugin.impl;


import com.zyc.common.entity.TouchConfigInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.plugin.dao.TouchMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class TouchServiceImpl {

    public TouchConfigInfo selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            TouchMapper touchMapper = sqlSession.getMapper(TouchMapper.class);
            return touchMapper.selectOne(id);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null)
                sqlSession.close();
        }

        return null;
    }
}
