package com.zyc.plugin.calculate.impl;

import cn.hutool.core.lang.JarClassLoader;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ClassLoaderUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.FunctionInfo;
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
    }

    @Override
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
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
        String logStr="";
        String file_path=getFilePathByParam(this.param, this.dbConfig);
        try{

            //获取plugin code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String rule_id=run_jsmind_data.getOrDefault("rule_id", "").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            //调度逻辑时间,毫秒时间戳
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("数据库配置异常");
            }
            String base_path=dbConfig.get("file.path");
            file_path=getFilePath(base_path,group_id,group_instance_id,id);

            //生成参数
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

            //生成参数
            CalculateResult calculateResult = calculateResult(base_path, run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();

            file_path = getFilePath(file_dir, id);

            Map<String,Object> params = new HashMap<>();
            params.put("strategy_instance_id", id);
            params.put("strategy_instance", this.param);

            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
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
                        Object ret = executeFunction(functionInfo, objectMap, param_codes);
                        if(ret != null){
                            if(functionInfo == null || Lists.newArrayList("int","long","string").contains(functionInfo.getReturn_type().toLowerCase())){
                                rs3.add(ret.toString());
                            }else if(Lists.newArrayList("array").contains(functionInfo.getReturn_type().toLowerCase())){
                                rs3.add(StringUtils.join((Object[])ret, ","));
                            }else if(Lists.newArrayList("map").contains(functionInfo.getReturn_type().toLowerCase())){
                                rs3.add(StringUtils.join(((Map)ret).values(), ","));
                            }else if(Lists.newArrayList("boolean").contains(functionInfo.getReturn_type().toLowerCase())){
                                rs3.add(uid);
                            }
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                rs = rs3;
            }

            logStr = StrUtil.format("task: {}, calculate finish size: {}", id, rs.size());
            LogUtil.info(strategy_id, id, logStr);
            writeFileAndPrintLog(id,strategy_id, file_path, rs);
        }catch (Exception e){
            writeEmptyFile(file_path);
            setStatus(id, Const.STATUS_ERROR);
            LogUtil.error(strategy_id, id, e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
        }
    }

    public Object executeFunction(FunctionInfo functionInfo, Map<String, Object> objectMap, List<String> param_codes) throws ClassNotFoundException, IllegalAccessException, InstantiationException, ScriptException, NoSuchMethodException {
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
                Object ret = GroovyFactory.execExpress(function_script, objectMap);
                return ret;
            }else{
                Object clsInstance = ClassLoaderUtil.loadClass(function_class).newInstance();
                objectMap.put(clsInstanceName, clsInstance);
                function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(param_codes, ",")+")";
                Object ret = GroovyFactory.execExpress(function_script, objectMap);
                return ret;
            }
        }else if(!StringUtils.isEmpty(function_script)){
            Map<String,Object> stringObjectMap = new LinkedHashMap<>();
            for (String param_code: param_codes){
                stringObjectMap.put(param_code, objectMap.get(param_code));
            }
            Object ret = GroovyFactory.execExpress(function_script, function_name, stringObjectMap);
            return ret;
        }

        return null;
    }

}
