package com.zyc.magic_mirror.label.calculate.impl;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.InstanceType;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.DateUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CrowdRuleCalculateImpl extends  BaseCalculate{
    private static Logger logger= LoggerFactory.getLogger(CrowdRuleCalculateImpl.class);

    /**
     * "id" : 1032062601107869696,
     * 		"strategy_context" : "测试新标签",
     * 		"group_id" : "测试策略组",
     * 		"group_context" : "测试策略组",
     * 		"group_instance_id" : "1032062598209605632",
     * 		"instance_type" : "crowd_rule",
     * 		"start_time" : "2022-10-18 22:48:16",
     * 		"end_time" : null,
     * 		"jsmind_data" : "{\"crowd_rule_context\":\"测试新标签\",\"type\":\"crowd_rule\",\"time_out\":\"86400\",\"positionX\":303,\"positionY\":78,\"operate\":\"or\",\"crowd_rule\":\"985279445663223808\",\"touch_type\":\"database\",\"name\":\"测试新标签\",\"more_task\":\"crowd_rule\",\"id\":\"5e7_0c1_8f31_4a\",\"divId\":\"5e7_0c1_8f31_4a\",\"depend_level\":\"0\"}",
     * 		"owner" : "zyc",
     * 		"is_delete" : "0",
     * 		"create_time" : "2022-06-11 21:38:40",
     * 		"update_time" : "2022-10-18 22:48:18",
     * 		"expr" : null,
     * 		"misfire" : "0",
     * 		"priority" : "",
     * 		"status" : "create",
     * 		"quartz_time" : null,
     * 		"use_quartz_time" : null,
     * 		"time_diff" : null,
     * 		"schedule_source" : "2",
     * 		"cur_time" : "2022-10-18 22:48:16",
     * 		"run_time" : "2022-10-18 22:48:19",
     * 		"run_jsmind_data" : "{\"crowd_rule_context\":\"测试新标签\",\"type\":\"crowd_rule\",\"time_out\":\"86400\",\"positionX\":303,\"positionY\":78,\"operate\":\"or\",\"crowd_rule\":\"985279445663223808\",\"touch_type\":\"database\",\"name\":\"测试新标签\",\"more_task\":\"crowd_rule\",\"id\":\"5e7_0c1_8f31_4a\",\"divId\":\"5e7_0c1_8f31_4a\",\"depend_level\":\"0\"}",
     * 		"next_tasks" : "1032062601112064000",
     * 		"pre_tasks" : "1032062601124646912",
     * 		"is_disenable" : "false",
     * 		"depend_level" : "0",
     * 		"touch_type" : "database"
     *
     * run_jsmind_data 结构
     * {
     * 	"crowd_rule_context": "测试新标签",
     * 	"type": "crowd_rule",
     * 	"time_out": "86400",
     * 	"positionX": 303,
     * 	"positionY": 78,
     * 	"operate": "or",
     * 	"crowd_rule": "985279445663223808",
     * 	"touch_type": "database",
     * 	"name": "测试新标签",
     * 	"more_task": "crowd_rule",
     * 	"id": "5e7_0c1_8f31_4a",
     * 	"divId": "5e7_0c1_8f31_4a",
     * 	"depend_level": "0"
     * }
     * @param param
     * @param atomicInteger
     * @param dbConfig
     */
    public CrowdRuleCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        super(param, atomicInteger, dbConfig);
    }

    @Override
    public boolean checkSftp() {
        return Boolean.valueOf(this.dbConfig.getOrDefault("sftp.enable", "false"));
    }

    @Override
    public String storageMode() {
        return this.dbConfig.getOrDefault("storage.mode", "");
    }

    @Override
    public String getBucket() {
        return this.dbConfig.getOrDefault("storage.minio.bucket", super.getBucket());
    }

    @Override
    public String getRegion() {
        return this.dbConfig.getOrDefault("storage.minio.region", super.getRegion());
    }

    @Override
    public void process() {
        String logStr="";
        try{
            //客群id
            String crowd_rule_id=run_jsmind_data.get("crowd_rule").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            //调度逻辑时间,yyyy-MM-dd HH:mm:ss
            String cur_time= DateUtil.formatTime(strategyLogInfo.getCur_time());
            String base_path=strategyLogInfo.getBase_path();

            if(dbConfig==null){
                throw new Exception("客群信息数据库配置异常");
            }
            Set<String> rs = null;
            Set<DataPipe> rowsObj=Sets.newHashSet();
            if(is_disenable.equalsIgnoreCase("true")){
                //禁用任务不做处理,认为结果为空
                rs = Sets.newHashSet() ;
            }else{
                //解析客群,生成标签任务
                throw new Exception("暂时不支持客群任务计算");
            }
            rowsObj = Sets.newHashSet(rs.stream().map(s -> new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS)
                    .udata_type("").
                            task_type(InstanceType.CROWD_RULE.getCode()).build()).collect(Collectors.toSet()));

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rowsObj);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rowsObj, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            logger.error("label crowdrule run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }



    }
}
