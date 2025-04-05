package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.FilterInfo;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.plugin.dao.FilterMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FilterServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(FilterServiceImpl.class);

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
            logger.error("plugin filter selectByCode error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
