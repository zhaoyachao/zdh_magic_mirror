package com.zyc.magic_mirror.common.service.impl;

import com.zyc.magic_mirror.common.dao.PermissionUserMapper;
import com.zyc.magic_mirror.common.entity.PermissionUserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PermissionUserServiceImpl extends BaseServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(PermissionUserServiceImpl.class);

    public PermissionUserInfo selectUserInfo(String user_account, String product_code){

        return executeReadOnly(sqlSession -> {
            PermissionUserMapper permissionUserMapper = sqlSession.getMapper(PermissionUserMapper.class);
            PermissionUserInfo permissionUserInfo = permissionUserMapper.selectByAccount(user_account, product_code);
            return permissionUserInfo;
        });
    }
}
