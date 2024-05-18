package com.zyc.plugin.impl;


import com.zyc.common.entity.FunctionInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.plugin.dao.FunctionMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FunctionServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(FunctionServiceImpl.class);

    public FunctionInfo selectByFunction(String function_name){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            FunctionMapper functionMapper = sqlSession.getMapper(FunctionMapper.class);
            return functionMapper.selectOne(function_name);

        } catch (IOException e) {
            logger.error("plugin service selectByFunction error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
