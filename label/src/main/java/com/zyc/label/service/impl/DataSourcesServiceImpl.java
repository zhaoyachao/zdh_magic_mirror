package com.zyc.label.service.impl;

import com.zyc.common.entity.DataSourcesInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.label.dao.DataSourcesMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class DataSourcesServiceImpl {

    public DataSourcesInfo selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            DataSourcesMapper dataSourcesMapper = sqlSession.getMapper(DataSourcesMapper.class);

            DataSourcesInfo dataSourcesInfo=dataSourcesMapper.selectOne(id);

            return dataSourcesInfo;

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
