package com.zyc.magic_mirror.ship.dao;

import com.zyc.magic_mirror.common.entity.FunctionInfo;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.BaseMapper;

import java.util.List;


public interface FunctionMapper extends BaseMapper<FunctionInfo> {
    @Select({
            "select * from function_info where is_delete=0"
    })
    public List<FunctionInfo> selectAllNotDelete();
}
