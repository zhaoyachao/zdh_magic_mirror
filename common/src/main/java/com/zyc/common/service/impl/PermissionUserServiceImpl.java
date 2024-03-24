package com.zyc.common.service.impl;

import com.zyc.common.dao.PermissionUserMapper;
import com.zyc.common.entity.PermissionUserInfo;
import com.zyc.common.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;

public class PermissionUserServiceImpl {


    public PermissionUserInfo selectUserInfo(String user_account, String product_code){

        SqlSession sqlSession=null;
        try {
            sqlSession = MybatisUtil.getSqlSession();
            PermissionUserMapper permissionUserMapper = sqlSession.getMapper(PermissionUserMapper.class);
            PermissionUserInfo permissionUserInfo = permissionUserMapper.selectOne(user_account, product_code);
            return permissionUserInfo;
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null) {
                sqlSession.close();
            }
        }
        return null;
    }
}
