package com.zyc.ship.dao;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;


public interface StrategyGroupMapper {

    @Select({
            "select * from strategy_group_instance where group_type='online' and status='sub_task_dispatch'"
    })
    public List<Map<String,Object>> select();

    @Select({
            "select * from strategy_instance where group_instance_id=#{group_instance_id}"
    })
    public List<Map<String,Object>> selectStrategys(@Param("group_instance_id") String group_instance_id);

    @Update({
            "update strategy_instance set status='killed' where group_type='online' and status='kill'"
    })
    public int update2Killed();
}
