package com.zyc.label.dao;

import com.zyc.common.entity.DataSourcesInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface DataSourcesMapper {

    @Select({
            "select * from data_sources_info where id = #{id} "
    })
    public DataSourcesInfo selectOne(@Param("id") String id);
}
