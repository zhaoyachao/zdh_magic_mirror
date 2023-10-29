package com.zyc.variable.dao;

import com.zyc.common.entity.LabelInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;


public interface LabelMapper {

    @Select({
            "select * from label_info where label_use_type='single' and status='1'"
    })
    public List<LabelInfo> select();
}
