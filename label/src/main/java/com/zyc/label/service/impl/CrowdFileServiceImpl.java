package com.zyc.label.service.impl;

import com.zyc.common.entity.CrowdFileInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.label.dao.CrowdFileMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CrowdFileServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(CrowdFileServiceImpl.class);

    public CrowdFileInfo selectById(String id){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            CrowdFileMapper crowdFileMapper = sqlSession.getMapper(CrowdFileMapper.class);

            CrowdFileInfo crowdFileInfo=crowdFileMapper.selectOne(id);

            return crowdFileInfo;

        } catch (IOException e) {
            logger.error("crowd file service selectById error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
