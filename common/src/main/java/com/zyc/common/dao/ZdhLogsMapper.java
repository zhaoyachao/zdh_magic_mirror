package com.zyc.common.dao;

import com.zyc.common.entity.ZdhLogs;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;


public interface ZdhLogsMapper {

    @Insert({ "insert into zdh_logs(task_logs_id, job_id,log_time, msg, level) values(#{zdh_logs.task_logs_id}, #{zdh_logs.job_id},#{zdh_logs.log_time}, #{zdh_logs.msg}, #{zdh_logs.level})" })
    public int insert(@Param("zdh_logs") ZdhLogs zdh_logs);

}
