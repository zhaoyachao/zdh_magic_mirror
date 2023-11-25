package com.zyc.plugin.dao;

import com.zyc.common.entity.FunctionInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface FunctionMapper {

    @Select({
            "select * from function_info where function_name = #{function_name} and is_delete=0"
    })
    public FunctionInfo selectOne(@Param("function_name") String function_name);
}
