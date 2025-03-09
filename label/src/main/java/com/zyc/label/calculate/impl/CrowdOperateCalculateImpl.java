package com.zyc.label.calculate.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.zyc.common.entity.DataPipe;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.MybatisUtil;
import com.zyc.label.calculate.CrowdRuleCalculate;
import com.zyc.label.dao.StrategyInstanceMapper;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 运算符任务
 */
public class CrowdOperateCalculateImpl extends BaseCalculate implements CrowdRuleCalculate {
    private static Logger logger= LoggerFactory.getLogger(CrowdOperateCalculateImpl.class);
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

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
    public CrowdOperateCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        initJinJavaCommonParam(strategyLogInfo, this.param);
        String logStr="";
        try{
            //客群运算id
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String rule_context=run_jsmind_data.get("rule_context").toString();
            String operate=run_jsmind_data.get("operate").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            String status=run_jsmind_data.getOrDefault("data_status",Const.FILE_STATUS_SUCCESS).toString();//依赖数据状态,1:成功,2:失败,3:不区分

            //获取上游任务
            String pre_tasks = this.param.get("pre_tasks").toString();

            Set<DataPipe> rs = null;
            if(is_disenable.equalsIgnoreCase("true")){
                //禁用任务不做处理,认为结果为空
                rs = Sets.newHashSet();
            }else{
                String file_dir= getFileDir(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                        strategyLogInfo.getStrategy_group_instance_id());
                if(!StringUtils.isEmpty(pre_tasks)){
                    List<String> other = resetPreTasks(strategyLogInfo.getStrategy_instance_id(),pre_tasks, operate);
                    List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
                    //多个任务交并排逻辑
                    rs = calculate(strategyLogInfo, other, file_dir, operate, strategyInstances,status);
                }else{
                    throw new Exception("运算符节点至少依赖一个父节点");
                }
            }

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);

        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }

    private List<String> resetPreTasks(String task_id, String pre_tasks, String operate) throws Exception {
        List<String> other = new ArrayList<>(Arrays.asList(pre_tasks.split(",")));
        if(operate.equalsIgnoreCase("not")){
            //必须计算出主标签,且只能有一个主标签
            StrategyInstanceMapper strategyInstanceMappler = MybatisUtil.getSqlSession().getMapper(StrategyInstanceMapper.class);

            String base_id = "";
            List<StrategyInstance> list = strategyInstanceMappler.selectByIds(pre_tasks.split(","));
            for (StrategyInstance strategyInstance:list){
                String rjd = strategyInstance.getRun_jsmind_data();
                Map parseObject = JSON.parseObject(rjd, Map.class);
                if(parseObject.getOrDefault("is_base","false").toString().equalsIgnoreCase("true")){
                    base_id = strategyInstance.getId();
                    break;
                }
            }

            if(StringUtils.isEmpty(base_id)){
                throw  new Exception("无法找到base数据");
            }
            other.remove(base_id);
            logger.info("task: {}, base: {}, other: {}", task_id, base_id, String.join(",", other));
            other.add(0, base_id);
        }

        return other;
    }

}
