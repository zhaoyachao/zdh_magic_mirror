package com.zyc.common.service.impl;

import com.zyc.common.dao.NoticeMapper;
import com.zyc.common.entity.NoticeInfo;
import com.zyc.common.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

public class NoticeServiceImpl {

    public void send(NoticeInfo noticeInfo){

        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            NoticeMapper noticeMapper = sqlSession.getMapper(NoticeMapper.class);
            noticeInfo.setCreate_time(new Timestamp(new Date().getTime()));
            noticeInfo.setUpdate_time(new Timestamp(new Date().getTime()));
            noticeMapper.insert(noticeInfo);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null)
                sqlSession.close();
        }

    }
}
