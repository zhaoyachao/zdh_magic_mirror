package com.zyc.magic_mirror.plugin.dao;

import com.zyc.magic_mirror.common.entity.StrategyInstance;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;


public interface StrategyInstanceMapper {

    @Update({
            "<script>",
            "update strategy_instance ",
            "<set>",
            "<if test = 'status != null and status != \"\"'> ",
            "status=#{status} ,",
            "</if> ",
            "<if test = 'update_time != null'> ",
            "update_time=#{update_time} ,",
            "</if> ",
            "<if test = 'run_jsmind_data != null and run_jsmind_data != \"\"'> ",
            "run_jsmind_data=#{run_jsmind_data} ,",
            "</if> ",
            "</set>",
            "where id = #{id}",
            "</script>"
    }
    )
    public int updateStatusAndUpdateTimeById(StrategyInstance strategyInstance);

    @Select({
            "<script>",
            "select * from strategy_instance where id in ",
            "<foreach collection='array' item='id' open='(' separator=',' close=')'>",
            "#{id}",
            "</foreach>",
            "</script>"
    })
    public List<StrategyInstance> selectByIds(String[] ids);

    @Select({
            "<script>",
            "select * from strategy_instance where status in ",
            "<foreach collection='statusAry' item='status' open='(' separator=',' close=')'>",
            "#{status}",
            "</foreach>",
            " and instance_type in ",
            "<foreach collection='instance_types' item='instance_type' open='(' separator=',' close=')'>",
            "#{instance_type}",
            "</foreach>",
            " and group_type = #{group_type}",
            "order by priority desc, start_time asc",
            "</script>"
    })
    public List<StrategyInstance> selectByStatus(@Param("statusAry") String[] status, @Param("instance_types") String[] instance_type, @Param("group_type") String group_type);

    @Update({
            "<script>",
            "update strategy_instance set status='check_dep_finish'",
            "where ",
            "status=#{status}",
            " and instance_type in ",
            "<foreach collection='instance_types' item='instance_type' open='(' separator=',' close=')'>",
            "#{instance_type}",
            "</foreach>",
            "and group_type='offline'",
            "</script>"
    }
    )
    public int updateStatus2CheckFinish(@Param("status")String status, @Param("instance_types") String[] instance_types);

    @Update({
            "<script>",
            "update strategy_instance set status='check_dep_finish'",
            "where ",
            "status=#{status}",
            " and instance_type in ",
            "<foreach collection='instance_types' item='instance_type' open='(' separator=',' close=')'>",
            "#{instance_type}",
            "</foreach>",
            "and group_type='offline'",
            "and (strategy_id % #{total_slot} &gt;= #{start_slot} and strategy_id % #{total_slot} &lt; #{end_slot})",
            "</script>"
    }
    )
    public int updateStatus2CheckFinishBySlot(@Param("status")String status, @Param("instance_types") String[] instance_types, @Param("start_slot")int start_slot, @Param("end_slot")int end_slot, @Param("total_slot")int total_slot);
}
