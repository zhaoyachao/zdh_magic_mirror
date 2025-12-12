package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.label.dao.StrategyInstanceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StrategyInstanceServiceImpl extends BaseServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(StrategyInstanceServiceImpl.class);

    public int updateStatusAndUpdateTimeById(StrategyInstance strategyInstance){
        return executeTransaction(sqlSession -> {
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            return strategyInstanceMappler.updateStatusAndUpdateTimeById(strategyInstance);
        });
    }

    public int updateStatusAndUpdateTimeByIdAndOldStatus(StrategyInstance strategyInstance, String oldStatus){
        return executeTransaction(sqlSession -> {
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            return strategyInstanceMappler.updateStatusAndUpdateTimeByIdAndOldStatus(strategyInstance, oldStatus);
        });
    }


    public List<StrategyInstance> selectByStatus(String[] status, String[] instance_type){
        return executeReadOnly(sqlSession -> {
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            return strategyInstanceMappler.selectByStatus(status, instance_type,"offline");
        });
    }

    public List<StrategyInstance> selectByIds(String[] ids){
        return executeReadOnly(sqlSession -> {
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            return strategyInstanceMappler.selectByIds(ids);
        });
    }

    public int updateStatus2CheckFinish(String status, String[] instance_types){
        return executeTransaction(sqlSession -> {
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            return strategyInstanceMappler.updateStatus2CheckFinish(status, instance_types);
        });
    }

    public int updateStatus2CheckFinishBySlot(String status, String[] instance_types, int start_slot, int end_slot, int total_slot){
        return executeTransaction(sqlSession -> {
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            return strategyInstanceMappler.updateStatus2CheckFinishBySlot(status, instance_types, start_slot, end_slot, total_slot);
        });
    }
}
