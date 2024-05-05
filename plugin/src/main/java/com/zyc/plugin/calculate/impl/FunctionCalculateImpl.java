package com.zyc.plugin.calculate.impl;

import cn.hutool.core.lang.JarClassLoader;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ClassLoaderUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.FunctionInfo;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.groovy.GroovyFactory;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.FunctionCalculate;
import com.zyc.plugin.impl.FunctionServiceImpl;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 函数实现
 */
public class FunctionCalculateImpl extends BaseCalculate implements FunctionCalculate {
    private static Logger logger= LoggerFactory.getLogger(FunctionCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1177757911275802624,
     "strategy_context" : "(and)首字母小写",
     "group_id" : "1000709645808963584",
     "group_context" : "测试策略组",
     "group_instance_id" : "1177757911217082368",
     "instance_type" : "function",
     "start_time" : "2023-11-24 23:49:21",
     "end_time" : "2023-01-18 08:00:00",
     "jsmind_data" : "{\"rule_param\":\"[{\\\"param_code\\\":\\\"str\\\",\\\"param_context\\\":\\\"输入字符串\\\",\\\"param_value\\\":\\\"ABC\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"function\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"首字母小写\",\"positionX\":522,\"rule_id\":\"uncapitalize\",\"positionY\":378,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)首字母小写\",\"more_task\":\"function\",\"id\":\"1177757607033573376\",\"divId\":\"1177757607033573376\",\"depend_level\":\"0\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2023-11-24 23:49:28",
     "update_time" : "2023-11-24 23:49:28",
     "expr" : "0 * * * * ? *",
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : "2023-11-04 18:23:00",
     "use_quartz_time" : "on",
     "time_diff" : "",
     "schedule_source" : "2",
     "cur_time" : "2023-11-24 23:49:21",
     "run_time" : "2023-11-24 23:49:28",
     "run_jsmind_data" : "{\"rule_param\":\"[{\\\"param_code\\\":\\\"str\\\",\\\"param_context\\\":\\\"输入字符串\\\",\\\"param_value\\\":\\\"ABC\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"function\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"首字母小写\",\"positionX\":522,\"rule_id\":\"uncapitalize\",\"positionY\":378,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)首字母小写\",\"more_task\":\"function\",\"id\":\"1177757607033573376\",\"divId\":\"1177757607033573376\",\"depend_level\":\"0\"}",
     "next_tasks" : "",
     "pre_tasks" : "1177757911250636800",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "1177757607033573376",
     "group_type" : "offline",
     "data_node" : ""
     }
     ]}
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public FunctionCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
        getSftpUtil(this.dbConfig);
    }

    @Override
    public boolean checkSftp() {
        return Boolean.valueOf(this.dbConfig.getOrDefault("sftp.enable", "false"));
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
        String logStr="";
        try{

            //获取plugin code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String rule_id=run_jsmind_data.getOrDefault("rule_id", "").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());
            String return_value_express = run_jsmind_data.getOrDefault("return_value_express", "ret").toString();
            String return_diff_enable = run_jsmind_data.getOrDefault("return_diff_enable", "false").toString();
            String return_value_type = run_jsmind_data.getOrDefault("return_value_type", "string").toString();
            String return_operate = run_jsmind_data.getOrDefault("return_operate", "").toString();
            String return_operate_value = run_jsmind_data.getOrDefault("return_operate_value", "").toString();

            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();

            Map<String,Object> params = new HashMap<>();
            params.put("strategy_instance_id", strategyLogInfo.getStrategy_instance_id());
            params.put("strategy_instance", this.param);

            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数规则: "+gson.toJson(params));
                Jinjava jinjava=new Jinjava();
                //获取函数信息
                FunctionServiceImpl functionService = new FunctionServiceImpl();
                FunctionInfo functionInfo = functionService.selectByFunction(rule_id);

                Set<String> rs3 = Sets.newHashSet();
                for(String uid: rs){
                    try{
                        Map<String, Object> objectMap = new HashMap<>();
                        List<String> param_codes = new ArrayList<>();
                        objectMap.putAll(params);
                        objectMap.put("uid", uid);//获取当前结果集信息

                        for(Map map: rule_params){
                            String param_code = map.get("param_code").toString();
                            String param_value = map.get("param_value").toString();
                            String new_param_value = jinjava.render(param_value, objectMap);//替换可变参数
                            objectMap.put(param_code, new_param_value);
                            param_codes.add(param_code);
                        }
                        Object ret = executeFunction(strategyLogInfo,functionInfo, objectMap, param_codes);
                        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "新增结果变量ret, 需要从结果变量取值,可对ret进行操作");
                        //判断是否有返回值diff
                        Object ret_diff = execReturnDiffExpress(strategyLogInfo, ret,  return_value_express, return_operate, return_operate_value, return_diff_enable, objectMap);
                        if(return_diff_enable.equalsIgnoreCase("true")){
                            //开启对比,结果为true表示成功数据,false为失败数据
                            if(ret_diff.toString().equalsIgnoreCase("true")){
                                rs3.add(uid);
                            }
                        }else{
                            //未开启结果对比,直接返回表达式
                            if(ret_diff != null){
                                rs3.add(ret_diff.toString());
                            }
                        }

//                        if(ret != null){
//                            if(functionInfo == null || Lists.newArrayList("int","long","string").contains(functionInfo.getReturn_type().toLowerCase())){
//                                rs3.add(ret.toString());
//                            }else if(Lists.newArrayList("array").contains(functionInfo.getReturn_type().toLowerCase())){
//                                rs3.add(StringUtils.join((Object[])ret, ","));
//                            }else if(Lists.newArrayList("map").contains(functionInfo.getReturn_type().toLowerCase())){
//                                rs3.add(StringUtils.join(((Map)ret).values(), ","));
//                            }else if(Lists.newArrayList("boolean").contains(functionInfo.getReturn_type().toLowerCase())){
//                                rs3.add(uid);
//                            }
//                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                rs = rs3;
            }
            Set<String> rs_error = Sets.difference(calculateResult.getRs(), rs);
            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }

    public Object executeFunction(StrategyLogInfo strategyLogInfo,FunctionInfo functionInfo, Map<String, Object> objectMap, List<String> param_codes) throws ClassNotFoundException, IllegalAccessException, InstantiationException, ScriptException, NoSuchMethodException {
        String function_name = functionInfo.getFunction_name();
        String function_class = functionInfo.getFunction_class();
        String function_load_path = functionInfo.getFunction_load_path();
        String function_script = functionInfo.getFunction_script();

        if(!StringUtils.isEmpty(function_class)){
            String[] function_packages = function_class.split(",");
            String clsName = ArrayUtil.get(function_packages, function_packages.length-1);
            String clsInstanceName = StringUtils.uncapitalize(clsName);

            //加载三方工具类
            if(!StringUtils.isEmpty(function_load_path)){
                JarClassLoader jarClassLoader = JarClassLoader.loadJar(new File(function_load_path));
                Class cls = jarClassLoader.loadClass(function_class);
                Object clsInstance = cls.newInstance();
                objectMap.put(clsInstanceName, clsInstance);
                function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(param_codes, ",")+")";
                LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数: "+function_script+", 参数: "+JSON.toJSONString(objectMap));
                Object ret = GroovyFactory.execExpress(function_script, objectMap);
                return ret;
            }else{
                Object clsInstance = ClassLoaderUtil.loadClass(function_class).newInstance();
                objectMap.put(clsInstanceName, clsInstance);
                function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(param_codes, ",")+")";
                LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数: "+function_script+", 参数: "+JSON.toJSONString(objectMap));
                Object ret = GroovyFactory.execExpress(function_script, objectMap);
                return ret;
            }
        }else if(!StringUtils.isEmpty(function_script)){
            Map<String,Object> stringObjectMap = new LinkedHashMap<>();
            for (String param_code: param_codes){
                stringObjectMap.put(param_code, objectMap.get(param_code));
            }
            LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数: "+function_script+", 参数: "+JSON.toJSONString(stringObjectMap));
            Object ret = GroovyFactory.execExpress(function_script, function_name, stringObjectMap);
            return ret;
        }

        return null;
    }

    public Object execReturnDiffExpress(StrategyLogInfo strategyLogInfo,Object ret, String return_value_express, String return_operate, String return_operate_value, String return_diff_enable, Map<String, Object> objectMap) throws ScriptException, NoSuchMethodException {

        Map<String, Object> tmp = new HashMap<>();
        Jinjava jinjava=new Jinjava();
        String function_name = "plugin_function_if_v1";
        String str_pre="";
        String str_suffix="";



        //解析return_operate_value
        String new_return_operate_value = jinjava.render(return_operate_value, tmp);

        objectMap.put("ret", ret);
        objectMap.put(new_return_operate_value, new_return_operate_value);

        String function_script = "if({{return_value_express}} {{operate}} "+new_return_operate_value+") return true else return false";

        if(return_operate.equalsIgnoreCase("in")){
            function_name = "plugin_function_if_v2";
            function_script = "if({{return_value_express}}.contains("+new_return_operate_value+")) return true else return false";
        }

        if(return_operate.equalsIgnoreCase("neq")){
            tmp.put("operate", "!=");//取值表达式
            function_script = "if({{return_value_express}} {{operate}} "+new_return_operate_value+") return true else return false";
        }
        if(return_operate.equalsIgnoreCase("eq")){
            tmp.put("operate", "==");//取值表达式
            function_script = "if({{return_value_express}} {{operate}} "+new_return_operate_value+") return true else return false";
        }
        if(return_operate.equalsIgnoreCase("gt")){
            tmp.put("operate", ">");//取值表达式
        }
        if(return_operate.equalsIgnoreCase("lt")){
            tmp.put("operate", "<");//取值表达式
        }
        if(return_operate.equalsIgnoreCase("gte")){
            tmp.put("operate", ">=");//取值表达式
        }
        if(return_operate.equalsIgnoreCase("lte")){
            tmp.put("operate", "<=");//取值表达式
        }

        if(return_diff_enable.equalsIgnoreCase("true")){

        }else{
            //直接获取表达式
            function_name = "plugin_function_if_v0";
            function_script = "return "+new_return_operate_value;
        }

        tmp.put("return_value_express", return_value_express);//取值表达式

        function_script = jinjava.render(function_script, tmp);//替换可变参数

        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "结果对比函数: "+function_name+", "+function_script+", 参数: "+JSON.toJSONString(objectMap));

        Object obj = GroovyFactory.execExpress(function_script, objectMap);

        return obj;

    }


}
