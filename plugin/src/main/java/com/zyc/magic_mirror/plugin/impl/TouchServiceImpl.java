package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.TouchConfigInfo;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.plugin.dao.TouchMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TouchServiceImpl {
    private Logger logger= LoggerFactory.getLogger(this.getClass());

    public TouchConfigInfo selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            TouchMapper touchMapper = sqlSession.getMapper(TouchMapper.class);
            return touchMapper.selectOne(id);

        } catch (IOException e) {
            logger.error("touch service selectById error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
