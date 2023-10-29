package com.zyc.plugin.dao;

import com.zyc.common.entity.TouchConfigInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface TouchMapper {

    @Select({
            "select * from touch_config_info where id = #{id} "
    })
    public TouchConfigInfo selectOne(@Param("id") String id);
}
