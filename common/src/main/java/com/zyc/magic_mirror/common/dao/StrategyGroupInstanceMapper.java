package com.zyc.magic_mirror.common.dao;

import com.zyc.magic_mirror.common.entity.StrategyGroupInstance;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface StrategyGroupInstanceMapper {
    @Select({
            "select * from strategy_group_instance where id=#{id}"
    })
    public StrategyGroupInstance selectOne(@Param("id") String id);
}
