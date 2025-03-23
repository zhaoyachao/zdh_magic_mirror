package com.zyc.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.DataPipe;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.DateUtil;
import com.zyc.common.util.JsonUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.RightsCalculate;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 权益实现
 */
public class RightsCalculateImpl extends BaseCalculate implements RightsCalculate {
    private static Logger logger= LoggerFactory.getLogger(RightsCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1140075021494915074,
     "strategy_context" : "权益1",
     "group_id" : "1000709645808963584",
     "group_context" : "测试策略组",
     "group_instance_id" : "1140075021478137856",
     "instance_type" : "rights",
     "start_time" : "2023-08-13 00:11:05",
     "end_time" : "2023-01-18 08:00:00",
     "jsmind_data" : "{\"rights_context\":\"权益1\",\"rights_param\":\"[{\\\"rights_template\\\":\\\"11\\\",\\\"rights_name\\\":\\\"22\\\",\\\"rights_value\\\":\\\"333\\\"}]\",\"type\":\"rights\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"positionX\":258,\"positionY\":536,\"touch_type\":\"database\",\"rights\":\"561_4d4_9337_2e\",\"name\":\"权益1\",\"more_task\":\"rights\",\"id\":\"750_74a_9f94_66\",\"divId\":\"750_74a_9f94_66\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2023-08-13 00:11:07",
     "update_time" : "2023-08-13 00:11:07",
     "expr" : "",
     "misfire" : "0",
     "priority" : "",
     "status" : "check_dep_finish",
     "quartz_time" : null,
     "use_quartz_time" : null,
     "time_diff" : "",
     "schedule_source" : "2",
     "cur_time" : "2023-08-13 00:11:05",
     "run_time" : "2023-08-13 00:11:07",
     "run_jsmind_data" : "{\"rights_context\":\"权益1\",\"rights_param\":\"[{\\\"rights_template\\\":\\\"11\\\",\\\"rights_name\\\":\\\"22\\\",\\\"rights_value\\\":\\\"333\\\"}]\",\"type\":\"rights\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"positionX\":258,\"positionY\":536,\"touch_type\":\"database\",\"rights\":\"561_4d4_9337_2e\",\"name\":\"权益1\",\"more_task\":\"rights\",\"id\":\"750_74a_9f94_66\",\"divId\":\"750_74a_9f94_66\"}",
     "next_tasks" : "",
     "pre_tasks" : "1140075021494915073",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "750_74a_9f94_66",
     "group_type" : "offline",
     "data_node" : ""
     }
     ]}
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public RightsCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
        getSftpUtil(this.dbConfig);
        initMinioClient(this.dbConfig);
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
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        initJinJavaCommonParam(strategyLogInfo, this.param);
        String logStr="";
        try{
            //获取标签code
            Map run_jsmind_data = JsonUtil.toJavaBean(this.param.get("run_jsmind_data").toString(), Map.class);
            String rights_param_str=run_jsmind_data.getOrDefault("rights_param","").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //调度逻辑时间,yyyy-MM-dd HH:mm:ss
            String cur_time= DateUtil.formatTime(strategyLogInfo.getCur_time());

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }
            String base_path=strategyLogInfo.getBase_path();

            //生成参数
            Gson gson=new Gson();
            List<Map> rights_param = gson.fromJson(rights_param_str, new TypeToken<List<Map>>(){}.getType());

            CalculateResult calculateResult = calculateResult(strategyLogInfo,base_path, run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();
            Set<DataPipe> rs_error = Sets.newHashSet();

            if(is_disenable.equalsIgnoreCase("true")){

            }else{
                //遍历权益,发放失败从rs集合中删除用户
                throw new Exception("当前权益模块未实现");
            }


            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin rights run error: ", e);
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }

}
