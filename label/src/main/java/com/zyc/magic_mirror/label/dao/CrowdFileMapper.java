package com.zyc.magic_mirror.label.dao;

import com.zyc.magic_mirror.common.entity.CrowdFileInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface CrowdFileMapper {

    @Select({
            "select * from crowd_file_info where id = #{id} "
    })
    public CrowdFileInfo selectOne(@Param("id") String id);
}
