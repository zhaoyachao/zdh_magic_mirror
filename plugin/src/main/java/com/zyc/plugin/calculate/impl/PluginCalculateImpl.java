package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.PluginInfo;
import com.zyc.common.plugin.PluginParam;
import com.zyc.common.plugin.PluginResult;
import com.zyc.common.plugin.PluginService;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.PluginCalculate;
import com.zyc.plugin.impl.KafkaPluginServiceImpl;
import com.zyc.plugin.impl.PluginServiceImpl;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 插件实现
 */
public class PluginCalculateImpl extends BaseCalculate implements PluginCalculate {
    private static Logger logger= LoggerFactory.getLogger(PluginCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1086945890049986561,
     "strategy_context" : "(and)kafka",
     "group_id" : "1086945820982382592",
     "group_context" : "测试kafka插件",
     "group_instance_id" : "1086945890016432128",
     "instance_type" : "plugin",
     "start_time" : "2023-03-19 09:34:53",
     "end_time" : "2023-03-19 09:33:30",
     "jsmind_data" : "{\"rule_expression_cn\":\"kafka\",\"rule_param\":\"[{\\\"param_code\\\":\\\"zk_url\\\",\\\"param_context\\\":\\\"zookeeper链接\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"127.0.0.1:2181\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"version\\\",\\\"param_context\\\":\\\"版本\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"1.0\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"plugin\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"kafka\",\"positionX\":264,\"rule_id\":\"kafka\",\"positionY\":358,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)kafka\",\"more_task\":\"plugin\",\"id\":\"1086945690535333888\",\"divId\":\"1086945690535333888\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2023-03-19 09:34:55",
     "update_time" : "2023-03-19 09:34:55",
     "expr" : "",
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : null,
     "use_quartz_time" : null,
     "time_diff" : "",
     "schedule_source" : "2",
     "cur_time" : "2023-03-19 09:34:53",
     "run_time" : "2023-03-19 09:34:55",
     "run_jsmind_data" : "{\"rule_expression_cn\":\"kafka\",\"rule_param\":\"[{\\\"param_code\\\":\\\"zk_url\\\",\\\"param_context\\\":\\\"zookeeper链接\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"127.0.0.1:2181\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"version\\\",\\\"param_context\\\":\\\"版本\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"1.0\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"plugin\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"kafka\",\"positionX\":264,\"rule_id\":\"kafka\",\"positionY\":358,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)kafka\",\"more_task\":\"plugin\",\"id\":\"1086945690535333888\",\"divId\":\"1086945690535333888\"}",
     "next_tasks" : "",
     "pre_tasks" : "1086945890049986560",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "1086945690535333888"
     }
     ]}
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public PluginCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
    }

    @Override
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        PluginServiceImpl pluginService=new PluginServiceImpl();
        //唯一任务ID
        String id=this.param.get("id").toString();
        String group_id=this.param.get("group_id").toString();
        String strategy_id=this.param.get("strategy_id").toString();
        String group_instance_id=this.param.get("group_instance_id").toString();
        String logStr="";
        String file_path="";
        try{

            //获取plugin code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String rule_id=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            //调度逻辑时间,毫秒时间戳
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }
            String base_path=dbConfig.get("file.path");

            //生成参数
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

            //生成参数
            CalculateResult calculateResult = calculateResult(base_path, run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();

            file_path = getFilePath(file_dir, id);

            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                PluginInfo pluginInfo = pluginService.selectById(rule_id);
                PluginService pluginServiceImpl = getPluginService(rule_id);
                //读取已经推送的信息
                List<String> his = readFile(file_dir+"/"+rule_id+"_"+id);
                Set<String> his2 = his.stream().map(str->str.split(",")[0]).collect(Collectors.toSet());
                Set<String> hisSet = Sets.newHashSet(his2);

                Set<String> diff = Sets.difference(rs, hisSet);
                Set<String> tmp = Sets.newHashSet();
                for (String s: diff){
                    PluginParam pluginParam = pluginServiceImpl.getPluginParam(rule_params);
                    PluginResult result = pluginServiceImpl.execute(pluginInfo, pluginParam, s);
                    tmp.add(s+","+result.getCode()+","+JSON.toJSONString(result.getResult())+","+System.currentTimeMillis());
                }
                writeFile(id,file_dir+"/"+rule_id+"_"+id, tmp);
            }

            String save_path = writeFile(id,file_path, rs);

            logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
            LogUtil.info(strategy_id, id, logStr);
            setStatus(id, "finish");
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


    public PluginService getPluginService(String plugin_code){
        if(plugin_code.equalsIgnoreCase("kafka")){
            return new KafkaPluginServiceImpl();
        }
        return null;
    }
}
