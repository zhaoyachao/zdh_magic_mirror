package com.zyc.plugin.impl;


import com.zyc.common.entity.FilterInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.plugin.dao.FilterMapper;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class FilterServiceImpl {

    public FilterInfo selectByCode(String filter_code){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            FilterMapper filterMapper = sqlSession.getMapper(FilterMapper.class);

            FilterInfo filterInfo=new FilterInfo();
            filterInfo.setFilter_code(filter_code);
            filterInfo.setIs_delete("0");

            return filterMapper.selectOne(filter_code);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
