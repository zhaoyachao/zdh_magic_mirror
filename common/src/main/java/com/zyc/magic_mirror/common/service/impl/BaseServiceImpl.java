package com.zyc.magic_mirror.common.service.impl;

import com.zyc.magic_mirror.common.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseServiceImpl {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 执行事务操作
     */
    protected <T> T executeTransaction(TransactionCallback<T> callback) {
        long startTime = System.currentTimeMillis();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        String className = this.getClass().getSimpleName();

        SqlSession sqlSession = null;
        try {
            logger.debug("开始执行事务操作: {}#{}", className, methodName);
            sqlSession = MybatisUtil.getSqlSession();
            T result = callback.doInTransaction(sqlSession);
            sqlSession.commit();
            long endTime = System.currentTimeMillis();
            logger.debug("事务操作执行成功: {}#{} 耗时: {}ms", className, methodName, (endTime - startTime));
            return result;
        } catch (Exception e) {
            logger.error("事务操作执行失败: {}#{} - 异常信息: {}", className, methodName, e.getMessage(), e);
            if (sqlSession != null) {
                sqlSession.rollback();
            }
            throw new RuntimeException("Transaction failed: " + e.getMessage(), e);
        } finally {
            // 确保SqlSession被关闭
            if (sqlSession != null) {
                try {
                    sqlSession.close();
                    logger.debug("SqlSession已关闭: {}#{}", className, methodName);
                } catch (Exception e) {
                    logger.error("关闭SqlSession失败: {}#{} - 异常信息: {}", className, methodName, e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 执行只读操作
     */
    protected <T> T executeReadOnly(ReadOnlyCallback<T> callback) {
        long startTime = System.currentTimeMillis();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        String className = this.getClass().getSimpleName();

        SqlSession sqlSession = null;
        try {
            logger.debug("开始执行只读操作: {}#{}", className, methodName);
            sqlSession = MybatisUtil.getSqlSession();
            T result = callback.doInReadOnly(sqlSession);
            long endTime = System.currentTimeMillis();
            logger.debug("只读操作执行成功: {}#{} 耗时: {}ms", className, methodName, (endTime - startTime));
            return result;
        } catch (Exception e) {
            logger.error("只读操作执行失败: {}#{} - 异常信息: {}", className, methodName, e.getMessage(), e);
            throw new RuntimeException("Read operation failed: " + e.getMessage(), e);
        } finally {
            // 确保SqlSession被关闭
            if (sqlSession != null) {
                try {
                    sqlSession.close();
                    logger.debug("SqlSession已关闭: {}#{}", className, methodName);
                } catch (Exception e) {
                    logger.error("关闭SqlSession失败: {}#{} - 异常信息: {}", className, methodName, e.getMessage(), e);
                }
            }
        }
    }

    @FunctionalInterface
    public interface TransactionCallback<T> {
        T doInTransaction(SqlSession sqlSession) throws Exception;
    }

    @FunctionalInterface
    public interface ReadOnlyCallback<T> {
        T doInReadOnly(SqlSession sqlSession) throws Exception;
    }
}
