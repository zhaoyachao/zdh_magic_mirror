package com.zyc.plugin.dao;

import com.zyc.common.entity.FilterInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface FilterMapper {

    @Select({
            "select * from filter_info where filter_code = #{filter_code} "
    })
    public FilterInfo selectOne(@Param("filter_code") String filter_code);
}
