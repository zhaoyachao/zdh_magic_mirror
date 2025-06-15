package com.zyc.magic_mirror.common.service.impl;


import com.zyc.magic_mirror.common.dao.StrategyGroupInstanceMapper;
import com.zyc.magic_mirror.common.entity.StrategyGroupInstance;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class StrategyGroupInstanceServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(StrategyGroupInstanceServiceImpl.class);

    public StrategyGroupInstance selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyGroupInstanceMapper strategyGroupInstanceMapper = sqlSession.getMapper(StrategyGroupInstanceMapper.class);
            StrategyGroupInstance result = strategyGroupInstanceMapper.selectOne(id);
            return result;

        } catch (IOException e) {
            logger.error("plugin service selectById error: ", e);
            return null;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("plugin service selectByIds sqlSession error: ", e);
                }
                sqlSession.close();
            }
        }
    }
}
