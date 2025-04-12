package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.DateUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TN实现,只做数据流转
 */
public class TnCalculateImpl extends BaseCalculate implements Runnable {
    private static Logger logger= LoggerFactory.getLogger(TnCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1167770916013215744,
     "strategy_context" : "(and)relative(2minute)",
     "group_id" : "1000709645808963584",
     "group_context" : "测试策略组",
     "group_instance_id" : "1167770915568619520",
     "instance_type" : "tn",
     "start_time" : "2023-10-28 10:24:41",
     "end_time" : "2023-01-18 08:00:00",
     "jsmind_data" : "{\"tn_type\":\"relative\",\"type\":\"tn\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"relative(2minute)\",\"positionX\":341,\"rule_id\":\"10f_2cf_807a_e2\",\"tn_unit\":\"minute\",\"positionY\":138,\"is_base\":\"false\",\"tn_value\":\"2\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)relative(2minute)\",\"more_task\":\"tn\",\"id\":\"1167770824732577792\",\"divId\":\"1167770824732577792\",\"depend_level\":\"0\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2023-10-28 10:24:42",
     "update_time" : "2023-10-28 10:24:42",
     "expr" : "0 0 * * * ? *",
     "misfire" : "0",
     "priority" : "",
     "status" : "finish",
     "quartz_time" : null,
     "use_quartz_time" : "on",
     "time_diff" : "",
     "schedule_source" : "2",
     "cur_time" : "2023-10-28 10:24:41",
     "run_time" : "2023-10-28 10:24:43",
     "run_jsmind_data" : "{\"tn_type\":\"relative\",\"type\":\"tn\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"relative(2minute)\",\"positionX\":341,\"rule_id\":\"10f_2cf_807a_e2\",\"tn_unit\":\"minute\",\"positionY\":138,\"is_base\":\"false\",\"tn_value\":\"2\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)relative(2minute)\",\"more_task\":\"tn\",\"id\":\"1167770824732577792\",\"divId\":\"1167770824732577792\",\"depend_level\":\"0\"}",
     "next_tasks" : "1167770915996438528",
     "pre_tasks" : "1167770915992244224",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "1167770824732577792",
     "group_type" : "offline",
     "data_node" : ""
     }
     ]}

     */

    public TnCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
    }

    @Override
    public void process() {
        String logStr="";
        try{

            //获取标签code
            Map run_jsmind_data = JsonUtil.toJavaBean(this.param.get("run_jsmind_data").toString(), Map.class);
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //调度逻辑时间,毫秒时间戳
            String cur_time= DateUtil.formatTime(strategyLogInfo.getCur_time());

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }
            String base_path=strategyLogInfo.getBase_path();


            CalculateResult calculateResult = calculateResult(strategyLogInfo, base_path, run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();

            if(is_disenable.equalsIgnoreCase("true")){

            }else{

            }

            //tn操作为整体操作,全部成功或者全部失败
            Set<DataPipe> rs_error = Sets.newHashSet();


            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin tn run error: ", e);
        }finally {

        }
    }

}
