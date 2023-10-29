package com.zyc.label.service.impl;

import com.zyc.common.util.MybatisUtil;
import com.zyc.label.dao.LabelMapper;
import com.zyc.common.entity.LabelInfo;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class LabelServiceImpl {

    public LabelInfo selectByCode(String label_code){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            LabelMapper labelMapper = sqlSession.getMapper(LabelMapper.class);

            LabelInfo labelInfo=new LabelInfo();
            labelInfo.setLabel_code(label_code);
            labelInfo.setIs_delete("0");
            labelInfo = labelMapper.selectOne(label_code);
            return labelInfo;

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null)
                sqlSession.close();
        }

        return null;
    }
}
