package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.FilterInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.plugin.dao.FilterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterServiceImpl extends BaseServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(FilterServiceImpl.class);

    public FilterInfo selectByCode(String filter_code){
        return executeReadOnly(sqlSession -> {
            FilterMapper filterMapper = sqlSession.getMapper(FilterMapper.class);
            return filterMapper.selectByFileCode(filter_code);
        });
    }
}
