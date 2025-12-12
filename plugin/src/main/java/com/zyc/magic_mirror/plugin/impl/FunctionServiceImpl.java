package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.FunctionInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.plugin.dao.FunctionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionServiceImpl extends BaseServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(FunctionServiceImpl.class);

    public FunctionInfo selectByFunction(String function_name){
        return executeReadOnly(sqlSession -> {
            FunctionMapper functionMapper = sqlSession.getMapper(FunctionMapper.class);
            return functionMapper.selectByFunction(function_name);
        });
    }
}
