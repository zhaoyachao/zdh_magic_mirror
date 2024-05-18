package com.zyc.label.service.impl;

import com.zyc.common.entity.DataSourcesInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.label.dao.DataSourcesMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataSourcesServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(DataSourcesServiceImpl.class);

    public DataSourcesInfo selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            DataSourcesMapper dataSourcesMapper = sqlSession.getMapper(DataSourcesMapper.class);

            DataSourcesInfo dataSourcesInfo=dataSourcesMapper.selectOne(id);

            return dataSourcesInfo;

        } catch (IOException e) {
            logger.error("label service selectById error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
