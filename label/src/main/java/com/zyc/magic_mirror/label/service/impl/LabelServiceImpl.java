package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.LabelInfo;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.label.dao.LabelMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LabelServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(LabelServiceImpl.class);

    public LabelInfo selectByCode(String label_code, String label_use_type){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            LabelMapper labelMapper = sqlSession.getMapper(LabelMapper.class);

            LabelInfo labelInfo=new LabelInfo();
            labelInfo.setLabel_code(label_code);
            labelInfo.setIs_delete("0");
            labelInfo.setLabel_use_type(label_use_type);
            labelInfo = labelMapper.selectOne(label_code, label_use_type);
            return labelInfo;

        } catch (IOException e) {
            logger.error("label service selectByCode error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
