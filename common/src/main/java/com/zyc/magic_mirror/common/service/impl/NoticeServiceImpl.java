package com.zyc.magic_mirror.common.service.impl;

import com.zyc.magic_mirror.common.dao.NoticeMapper;
import com.zyc.magic_mirror.common.entity.NoticeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class NoticeServiceImpl extends BaseServiceImpl{

    private static Logger logger= LoggerFactory.getLogger(NoticeServiceImpl.class);

    public void send(NoticeInfo noticeInfo){
        executeTransaction(sqlSession -> {
            NoticeMapper noticeMapper = sqlSession.getMapper(NoticeMapper.class);
            noticeInfo.setCreate_time(new Timestamp(System.currentTimeMillis()));
            noticeInfo.setUpdate_time(new Timestamp(System.currentTimeMillis()));
            return noticeMapper.insertSelective(noticeInfo);
        });

    }
}
