package com.zyc.magic_mirror.common.util;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;

public class MybatisUtil {

    private static SqlSessionFactory sqlSessionFactory;

    private static String sqlSessionLock = "sql_session_lock";
    private static SqlSessionFactory getSqlSessionFactory() throws IOException {
        if(sqlSessionFactory == null){
            synchronized (sqlSessionLock.intern()){
                if(sqlSessionFactory == null){
                    Reader reader = Resources.getResourceAsReader("mybatis.cfg.xml");
                    sqlSessionFactory=new SqlSessionFactoryBuilder().build(reader);
                }
            }
        }
        return sqlSessionFactory;
    }

    public static SqlSession getSqlSession() throws IOException{
        //填写参数 true表示事务自动提交
        return getSqlSessionFactory().openSession(true);
    }
}