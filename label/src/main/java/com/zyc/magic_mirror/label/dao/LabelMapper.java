package com.zyc.magic_mirror.label.dao;

import com.zyc.magic_mirror.common.entity.LabelInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.BaseMapper;


public interface LabelMapper extends BaseMapper<LabelInfo> {

    @Select({
            "select * from label_info where label_code = #{label_code} and label_use_type = #{label_use_type} and is_delete=0"
    })
    public LabelInfo selectByLabelCode(@Param("label_code") String label_code, @Param("label_use_type") String label_use_type);
}
