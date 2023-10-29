package com.zyc.label.dao;

import com.zyc.common.entity.LabelInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface LabelMapper {

    @Select({
            "select * from label_info where label_code = #{label_code} "
    })
    public LabelInfo selectOne(@Param("label_code") String label_code);
}
