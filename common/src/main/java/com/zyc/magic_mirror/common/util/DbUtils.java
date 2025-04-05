package com.zyc.magic_mirror.common.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class DbUtils {

    public static DataSource dataSource = null;

    static {
        try {
            Properties properties = new Properties();
            properties.load(DbUtils.class.getClassLoader().getResourceAsStream("application.properties"));

            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得数据库的链接
     *
     * @return 返回数据库链接
     */
    public Connection getConn() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public List<Map<String, Object>> R(String sql) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConn();
            preparedStatement = connection.prepareStatement(sql);

            resultSet = preparedStatement.executeQuery();

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            List<Map<String, Object>> result = new ArrayList<>();
            while (resultSet.next() != false) {
                //这里可以执行一些其他的操作
                Map<String, Object> rmap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    rmap.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                }
                result.add(rmap);
            }

            return result;

        } catch (Exception e) {
            // logger.error("类:"+Thread.currentThread().getStackTrace()[1].getClassName()+" 函数:"+Thread.currentThread().getStackTrace()[1].getMethodName()+ " 异常: {}", e);
            throw e;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void U(String table, Map<String,Object> where,Map<String,Object> params) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConn();
            List<String> p = new ArrayList<>();
            for (String key:params.keySet()){
                p.add(key+"='"+params.get(key)+"'");
            }
            String pstr = String.join(" , ", p);

            List<String> w = new ArrayList<>();
            for (String key:where.keySet()){
                w.add(key+"='"+where.get(key)+"'");
            }
            String wstr = String.join(" , ", w);

            String sql = String.format(" update %s set %s where %s", table, pstr, wstr);

            preparedStatement = connection.prepareStatement(sql);

            preparedStatement.execute();

        } catch (Exception e) {
            // logger.error("类:"+Thread.currentThread().getStackTrace()[1].getClassName()+" 函数:"+Thread.currentThread().getStackTrace()[1].getMethodName()+ " 异常: {}", e);
            throw e;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
