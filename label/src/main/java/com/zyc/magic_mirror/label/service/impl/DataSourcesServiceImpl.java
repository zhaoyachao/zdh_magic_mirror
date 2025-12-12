package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.DataSourcesInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.label.dao.DataSourcesMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourcesServiceImpl extends BaseServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(DataSourcesServiceImpl.class);

    public DataSourcesInfo selectById(String id){
        return executeReadOnly(sqlSession -> {
            DataSourcesMapper dataSourcesMapper = sqlSession.getMapper(DataSourcesMapper.class);
            return dataSourcesMapper.selectByPrimaryKey(id);
        });
    }
}
