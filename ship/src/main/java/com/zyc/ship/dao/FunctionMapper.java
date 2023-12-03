package com.zyc.ship.dao;

import com.zyc.common.entity.FunctionInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


public interface FunctionMapper {

    @Select({
            "select * from function_info where function_name = #{function_name} and is_delete=0"
    })
    public FunctionInfo selectOne(@Param("function_name") String function_name);

    @Select({
            "select * from function_info where is_delete=0"
    })
    public List<FunctionInfo> selectAll();
}
