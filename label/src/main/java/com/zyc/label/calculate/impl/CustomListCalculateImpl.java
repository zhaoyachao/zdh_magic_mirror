package com.zyc.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.DataSourcesInfo;
import com.zyc.common.entity.LabelInfo;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.DBUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.label.calculate.CustomListCalculate;
import com.zyc.label.service.impl.DataSourcesServiceImpl;
import com.zyc.label.service.impl.LabelServiceImpl;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用户名单实现
 */
public class CustomListCalculateImpl extends BaseCalculate implements CustomListCalculate {
    private static Logger logger= LoggerFactory.getLogger(CustomListCalculateImpl.class);

    /**
     * {
     * 	"owner": "zyc",
     * 	"schedule_source": "2",
     * 	"strategy_context": "(年龄 in 19)",
     * 	"create_time": 1658629372000,
     * 	"jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *        },
     * 	"run_time": 1660993147000,
     * 	"group_instance_id": "1010624036146778112",
     * 	"cur_time": 1660993145000,
     * 	"pre_tasks": "",
     * 	"group_context": "测试策略组",
     * 	"priority": "",
     * 	"is_disenable": "false",
     * 	"is_delete": "0",
     * 	"run_jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *    },
     * 	"start_time": 1660993145000,
     * 	"update_time": 1660993147000,
     * 	"group_id": "测试策略组",
     * 	"misfire": "0",
     * 	"next_tasks": "1010624036201304064",
     * 	"id": "1010624036209692673",
     * 	"instance_type": "label",
     * 	"depend_level": "0",
     * 	"status": "create"
     * }
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public CustomListCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        String strategy_id=this.param.get("strategy_id").toString();
        String group_instance_id=this.param.get("group_instance_id").toString();
        StrategyLogInfo strategyLogInfo = new StrategyLogInfo();
        strategyLogInfo.setStrategy_group_id(group_id);
        strategyLogInfo.setStrategy_id(strategy_id);
        strategyLogInfo.setStrategy_instance_id(id);
        strategyLogInfo.setStrategy_group_instance_id(group_instance_id);
        String logStr="";
        String file_path = "";
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String label_code=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            //调度逻辑时间,yyyy-MM-dd HH:mm:ss
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }

            String base_path=dbConfig.get("file.path");


            Set<String> rowsStr = Sets.newHashSet();
            //判断是否跳过类的策略,通过is_disenable=true,禁用的任务直接拉取上游任务的结果,并集(),交集(),排除()
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //生成参数
                String name_list_str = run_jsmind_data.getOrDefault("name_list","").toString();
                logStr = StrUtil.format("task: {}, param: {}", id, name_list_str);
                LogUtil.info(strategy_id, id, logStr);
                String[] name_list = name_list_str.split(",");
                if(name_list!=null && name_list.length>0){
                    rowsStr.addAll(Lists.newArrayList(name_list));
                }
            }

            Set<String> rs=Sets.newHashSet();
            //检查上游
            String pre_tasks = this.param.get("pre_tasks").toString();
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
            String file_dir= getFileDir(base_path,group_id,group_instance_id);
            String operate=run_jsmind_data.get("operate").toString();
            List<String> pre_tasks_list = Lists.newArrayList();
            if(!StringUtils.isEmpty(pre_tasks)){
                pre_tasks_list = Lists.newArrayList(pre_tasks.split(","));
            }
            rs = calculate(file_dir, pre_tasks_list, operate, rowsStr, strategyInstances, is_disenable);

            //执行sql,返回数据写文件
            file_path= getFilePath(base_path,group_id,group_instance_id,id);

            String save_path = writeFile(id,file_path, rs);
            logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
            LogUtil.info(strategy_id, id, logStr);
            setStatus(id, "finish");
            strategyLogInfo.setStatus("1");
            strategyLogInfo.setSuccess_num(String.valueOf(rs.size()));
            logStr = StrUtil.format("task: {}, update status finish", id);
            LogUtil.info(strategy_id, id, logStr);
        }catch (Exception e){
            writeEmptyFile(file_path);
            setStatus(id, "error");
            LogUtil.error(strategy_id, id, e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
        }
    }

}
