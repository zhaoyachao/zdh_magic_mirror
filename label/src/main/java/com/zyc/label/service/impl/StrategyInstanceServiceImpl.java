package com.zyc.label.service.impl;

import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.MybatisUtil;
import com.zyc.label.dao.StrategyInstanceMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class StrategyInstanceServiceImpl {


    public int updateByPrimaryKeySelective(StrategyInstance strategyInstance){
        SqlSession sqlSession=null;
        try {
            sqlSession=MybatisUtil.getSqlSession();
            StrategyInstanceMapper strategyInstanceMappler = sqlSession.getMapper(StrategyInstanceMapper.class);
            int result = strategyInstanceMappler.update(strategyInstance);
            return result;

        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    e.printStackTrace();
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
            e.printStackTrace();
            return null;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    e.printStackTrace();
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
            e.printStackTrace();
            return null;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    e.printStackTrace();
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
            e.printStackTrace();
            return 0;
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                sqlSession.close();
            }
        }
    }
}
