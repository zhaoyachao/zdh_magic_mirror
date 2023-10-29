package com.zyc.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
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
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        //唯一任务ID
        String id=this.param.get("id").toString();
        String group_id=this.param.get("group_id").toString();
        String group_instance_id=this.param.get("group_instance_id").toString();
        String logStr="";
        String file_path = "";
        try{
            String base_path=dbConfig.get("file.path");
            //客群运算id
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String crowd_operate_context=run_jsmind_data.get("crowd_operate_context").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //调度逻辑时间,yyyy-MM-dd HH:mm:ss
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("客群运算信息数据库配置异常");
            }
            //获取上游任务
            String pre_tasks = this.param.get("pre_tasks").toString();

            Set<String> rs = null;
            if(is_disenable.equalsIgnoreCase("true")){
                //禁用任务不做处理,认为结果为空
                rs = Sets.newHashSet() ;
            }else{
                String file_dir= getFileDir(base_path,group_id,group_instance_id);
                if(!StringUtils.isEmpty(pre_tasks)){
                    List<String> other = resetPreTasks(id,pre_tasks, crowd_operate_context);
                    List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
                    //多个任务交并排逻辑
                    rs = calculate(other, file_dir, crowd_operate_context, strategyInstances);
                }else{
                    throw new Exception("运算符节点至少依赖一个父节点");
                }
            }

            file_path = getFilePath(base_path,group_id,group_instance_id,id);

            String save_path = writeFile(id,file_path, rs);
            logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
            LogUtil.info(id, logStr);
            setStatus(id, "finish");
            logger.info("task: {}, update status finish", id);

        }catch (Exception e){
            atomicInteger.decrementAndGet();
            writeEmptyFile(file_path);
            setStatus(id,"error");
            LogUtil.error(id, e.getMessage());
            e.printStackTrace();
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
