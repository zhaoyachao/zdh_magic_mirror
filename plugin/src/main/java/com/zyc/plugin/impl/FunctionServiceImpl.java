package com.zyc.plugin.impl;


import com.zyc.common.entity.FunctionInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.plugin.dao.FunctionMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class FunctionServiceImpl {

    public FunctionInfo selectByFunction(String function_name){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            FunctionMapper functionMapper = sqlSession.getMapper(FunctionMapper.class);
            return functionMapper.selectOne(function_name);

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
