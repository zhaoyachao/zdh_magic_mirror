package com.zyc.magic_mirror.common.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import tk.mybatis.mapper.entity.Config;
import tk.mybatis.mapper.mapperhelper.MapperHelper;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public class MybatisUtil {

    private static SqlSessionFactory sqlSessionFactory;

    private static String sqlSessionLock = "sql_session_lock";
    private static SqlSessionFactory getSqlSessionFactory() throws IOException {
        if(sqlSessionFactory == null){
            synchronized (sqlSessionLock.intern()){
                if(sqlSessionFactory == null){
                    Properties dbProps = new Properties();
                    dbProps.load(Resources.getResourceAsStream("db.properties"));
                    HikariConfig hikariConfig = new HikariConfig(dbProps);
                    DataSource dataSource = new HikariDataSource(hikariConfig);
                    Reader reader = Resources.getResourceAsReader("mybatis.cfg.xml");
                    Configuration configuration =  new SqlSessionFactoryBuilder().build(reader).getConfiguration();

                    org.apache.ibatis.mapping.Environment environment = new org.apache.ibatis.mapping.Environment(
                            "development",  // 环境 ID（与 mybatis-config.xml 中一致）
                            new org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory(),  // 事务工厂
                            dataSource  // 注入 HikariCP 数据源
                    );

                    configuration.setEnvironment(environment);

                    // 2. 核心：手动初始化 TKMyBatis MapperHelper（绑定子类 SQL 提供器）
                    MapperHelper mapperHelper = new MapperHelper();
                    Config config = new Config();
                    config.setIDENTITY("MYSQL"); // 适配 MySQL 自增主键
                    mapperHelper.setConfig(config);
                    // 注册通用 Mapper，让 TKMyBatis 绑定正确的子类提供器
                    mapperHelper.registerMapper(tk.mybatis.mapper.common.Mapper.class);
                    // 关键：将 MapperHelper 绑定到 MyBatis 配置
                    mapperHelper.processConfiguration(configuration);

                    sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
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