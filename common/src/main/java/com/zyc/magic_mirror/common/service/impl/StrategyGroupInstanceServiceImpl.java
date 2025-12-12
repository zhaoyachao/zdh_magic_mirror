package com.zyc.magic_mirror.common.service.impl;

import com.zyc.magic_mirror.common.dao.StrategyGroupInstanceMapper;
import com.zyc.magic_mirror.common.entity.StrategyGroupInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StrategyGroupInstanceServiceImpl extends BaseServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(StrategyGroupInstanceServiceImpl.class);

    public StrategyGroupInstance selectById(String id){
        return executeReadOnly(sqlSession -> {
            StrategyGroupInstanceMapper strategyGroupInstanceMapper = sqlSession.getMapper(StrategyGroupInstanceMapper.class);
            return strategyGroupInstanceMapper.selectByPrimaryKey(id);
        });
    }
}
