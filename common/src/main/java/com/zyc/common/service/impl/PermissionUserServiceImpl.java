package com.zyc.common.service.impl;

import com.zyc.common.dao.PermissionUserMapper;
import com.zyc.common.entity.PermissionUserInfo;
import com.zyc.common.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PermissionUserServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(PermissionUserServiceImpl.class);

    public PermissionUserInfo selectUserInfo(String user_account, String product_code){

        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            PermissionUserMapper permissionUserMapper = sqlSession.getMapper(PermissionUserMapper.class);
            PermissionUserInfo permissionUserInfo = permissionUserMapper.selectOne(user_account, product_code);
            return permissionUserInfo;
        } catch (IOException e) {
            logger.error("permission service selectUserInfo error: ", e);
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }
        return null;
    }
}
