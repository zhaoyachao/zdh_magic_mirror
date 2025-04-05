package com.zyc.magic_mirror.common.dao;

import com.zyc.magic_mirror.common.entity.PermissionUserInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface PermissionUserMapper {
    @Select({
            "select * from permission_user_info where user_account = #{user_account} and product_code=#{product_code} and enable=true"
    })
    public PermissionUserInfo selectOne(@Param("user_account") String user_account, @Param("product_code") String product_code);
}
