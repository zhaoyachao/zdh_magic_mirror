package com.zyc.magic_mirror.variable.dao;

import com.zyc.magic_mirror.common.entity.LabelInfo;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.BaseMapper;

import java.util.List;


public interface LabelMapper extends BaseMapper<LabelInfo> {

    @Select({
            "select * from label_info where label_use_type='single' and status='1'"
    })
    public List<LabelInfo> selectList();
}
