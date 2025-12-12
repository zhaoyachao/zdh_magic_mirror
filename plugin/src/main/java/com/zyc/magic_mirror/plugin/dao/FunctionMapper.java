package com.zyc.magic_mirror.plugin.dao;

import com.zyc.magic_mirror.common.entity.FunctionInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.BaseMapper;


public interface FunctionMapper extends BaseMapper<FunctionInfo> {

    @Select({
            "select * from function_info where function_name = #{function_name} and is_delete=0"
    })
    public FunctionInfo selectByFunction(@Param("function_name") String function_name);
}
