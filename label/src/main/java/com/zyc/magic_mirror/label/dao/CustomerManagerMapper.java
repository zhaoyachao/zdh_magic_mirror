package com.zyc.magic_mirror.label.dao;

import com.zyc.magic_mirror.common.entity.CustomerManagerInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface CustomerManagerMapper {

    @Select({
            "select * from customer_manager_info where product_code = #{product_code} and uid_type = #{uid_type} and source=#{source} and is_delete=0"
    })
    public List<CustomerManagerInfo> select(@Param("product_code") String product_code, @Param("uid_type") String uid_type, @Param("source") String source);
}
