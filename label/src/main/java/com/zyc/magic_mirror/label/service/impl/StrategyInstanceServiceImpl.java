package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.label.dao.StrategyInstanceMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class StrategyInstanceServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(StrategyInstanceServiceImpl.class);

    public int updateByPrimaryKeySelective(StrategyInstance strategyInstance){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            int result = strategyInstanceMappler.update(strategyInstance);
            return result;

        } catch (IOException e) {
            logger.error("label service updateByPrimaryKeySelective error: ", e);
            return 0;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("label service updateByPrimaryKeySelective sqlSession error: ", e);
                }
                sqlSession.close();
            }
        }
    }


    public List<StrategyInstance> selectByStatus(String[] status, String[] instance_type){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            List<StrategyInstance> result = strategyInstanceMappler.selectByStatus(status, instance_type,"offline");
            return result;

        } catch (IOException e) {
            logger.error("label service selectByStatus error: ", e);
            return null;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("label service selectByStatus sqlSession error: ", e);
                }
                sqlSession.close();
            }
        }
    }

    public List<StrategyInstance> selectByIds(String[] ids){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            List<StrategyInstance> result = strategyInstanceMappler.selectByIds(ids);
            return result;

        } catch (IOException e) {
            logger.error("label service selectByIds error: ", e);
            return null;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("label service selectByIds error: ", e);
                }
                sqlSession.close();
            }
        }
    }

    public int updateStatus2CheckFinish(String status, String[] instance_types){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            int result = strategyInstanceMappler.updateStatus2CheckFinish(status, instance_types);
            return result;

        } catch (IOException e) {
            logger.error("label service updateStatus2CheckFinish error: ", e);
            return 0;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("label service updateStatus2CheckFinish sqlSession error: ", e);
                }
                sqlSession.close();
            }
        }
    }

    public int updateStatus2CheckFinishBySlot(String status, String[] instance_types, int start_slot, int end_slot, int total_slot){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            int result = strategyInstanceMappler.updateStatus2CheckFinishBySlot(status, instance_types, start_slot, end_slot, total_slot);
            return result;

        } catch (IOException e) {
            logger.error("label service updateStatus2CheckFinish error: ", e);
            return 0;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("label service updateStatus2CheckFinish sqlSession error: ", e);
                }
                sqlSession.close();
            }
        }
    }
}
