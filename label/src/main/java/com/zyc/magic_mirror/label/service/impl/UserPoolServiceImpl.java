package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.CustomerManagerInfo;
import com.zyc.magic_mirror.common.entity.LabelInfo;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.label.dao.CustomerManagerMapper;
import com.zyc.magic_mirror.label.dao.LabelMapper;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class UserPoolServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(UserPoolServiceImpl.class);

    public List<CustomerManagerInfo> select(String product_code, String uid_type, String source){
        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            CustomerManagerMapper customerManagerMapper = sqlSession.getMapper(CustomerManagerMapper.class);


            List<CustomerManagerInfo> customerManagerInfos = customerManagerMapper.select(product_code, uid_type, source);
            return customerManagerInfos;

        } catch (IOException e) {
            logger.error("userPool service select error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }

        return null;
    }
}
