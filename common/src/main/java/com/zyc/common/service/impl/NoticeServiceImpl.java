package com.zyc.common.service.impl;

import com.zyc.common.dao.NoticeMapper;
import com.zyc.common.entity.NoticeInfo;
import com.zyc.common.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;

public class NoticeServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(NoticeServiceImpl.class);

    public void send(NoticeInfo noticeInfo){

        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            NoticeMapper noticeMapper = sqlSession.getMapper(NoticeMapper.class);
            noticeInfo.setCreate_time(new Timestamp(System.currentTimeMillis()));
            noticeInfo.setUpdate_time(new Timestamp(System.currentTimeMillis()));
            noticeMapper.insert(noticeInfo);
        } catch (IOException e) {
            logger.error("notice service send error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

    }
}
