package com.zyc.magic_mirror.plugin.calculate.impl;

import cn.hutool.core.lang.JarClassLoader;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ClassLoaderUtil;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.FunctionInfo;
import com.zyc.magic_mirror.common.entity.StrategyLogInfo;
import com.zyc.magic_mirror.common.groovy.GroovyFactory;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import com.zyc.magic_mirror.plugin.impl.FunctionServiceImpl;
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
public class FunctionCalculateImpl extends BaseCalculate {
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
    public FunctionCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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

            //获取plugin code
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
            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();

            Map<String,Object> params = getJinJavaCommonParam();
            params.put("rule_params", rule_params);

            //mergeMapByVarPool(strategyLogInfo.getStrategy_group_instance_id(), params);


            Set<DataPipe> rs_error = Sets.newHashSet();
            Set<DataPipe> rs3 = Sets.newHashSet();

            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数规则: "+gson.toJson(params));
                Jinjava jinjava=new Jinjava();
                //获取函数信息
                FunctionServiceImpl functionService = new FunctionServiceImpl();
                FunctionInfo functionInfo = functionService.selectByFunction(rule_id);

                for(DataPipe r: rs){
                    try{
                        Map<String, Object> objectMap = new HashMap<>();
                        List<String> param_codes = new ArrayList<>();
                        objectMap.putAll(params);
                        objectMap.put("uid", r.getUdata());//获取当前结果集信息
                        objectMap.put("udata", r.getUdata());//获取当前结果集信息

                        for(Map map: rule_params){
                            String param_code = map.get("param_code").toString();
                            String param_type = map.getOrDefault("param_type", "").toString();
                            String param_value = map.get("param_value").toString();
                            String new_param_value = jinjava.render(param_value, objectMap);//替换可变参数

                            if(param_type.equalsIgnoreCase("int")){
                                objectMap.put(param_code, Integer.valueOf(new_param_value));
                            }else if(param_type.equalsIgnoreCase("long")){
                                objectMap.put(param_code, Long.valueOf(new_param_value));
                            }else if(param_type.equalsIgnoreCase("boolean")){
                                objectMap.put(param_code, Boolean.valueOf(new_param_value));
                            }else if(param_type.equalsIgnoreCase("array")){
                                objectMap.put(param_code, new_param_value.split(","));
                            }else if(param_type.equalsIgnoreCase("map")){
                                objectMap.put(param_code, JsonUtil.toJavaMap(new_param_value));
                            }else if(param_type.equalsIgnoreCase("object")){
                                objectMap.put(param_code, (Object)new_param_value);
                            }else if(param_type.equalsIgnoreCase("list")){
                                objectMap.put(param_code, JsonUtil.toJavaList(new_param_value));
                            }else if(param_type.equalsIgnoreCase("set")){
                                objectMap.put(param_code, JsonUtil.toJavaBean(new_param_value, Set.class));
                            }else{
                                objectMap.put(param_code, new_param_value);
                            }

                            objectMap.put(param_code, new_param_value);
                            param_codes.add(param_code);
                        }
                        Object ret = executeFunction(strategyLogInfo,functionInfo, objectMap, param_codes);
                        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "新增结果变量ret, 需要从结果变量取值,可对ret进行操作");
                        if(return_diff_enable.equalsIgnoreCase("true")){
                            Object ret_diff = execReturnDiffExpress(strategyLogInfo, ret, return_operate_value, objectMap);
                            //开启对比,结果为true表示成功数据,false为失败数据, 不等于true则跳过当前
                            if(!ret_diff.toString().equalsIgnoreCase("true")){
                                r.setStatus(Const.FILE_STATUS_FAIL);
                                r.setStatus_desc("function error ");
                                Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(r.getExt());
                                stringObjectMap.put("function_ret", ret instanceof String?ret.toString():JsonUtil.formatJsonString(ret));
                                rs_error.add(r);
                                continue;
                            }
                        }else{
                            //未开启对比,不做任何逻辑
                        }

                        if(return_value_express.trim().equalsIgnoreCase("uid")){
                            rs3.add(r);
                        }else if(return_value_express.trim().equalsIgnoreCase("ret")){
                            r.setUdata(ret.toString());
                            rs3.add(r);
                        }else{
                            Object new_ret = execFunctionExpress(strategyLogInfo, ret, return_value_express, objectMap);
                            if(new_ret != null && !StringUtils.isEmpty(new_ret.toString())){
                                r.setUdata(new_ret.toString());
                                rs3.add(r);
                            }else{
                                r.setStatus(Const.FILE_STATUS_FAIL);
                                r.setStatus_desc("function error ");
                                Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(r.getExt());
                                stringObjectMap.put("function_ret", ret instanceof String?ret.toString():JsonUtil.formatJsonString(ret));
                                rs_error.add(r);
                            }
                        }
                    }catch (Exception e){
                        logger.error("plugin function functionexecute error: ", e);
                    }
                }
            }
            rs = rs3;

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin function run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }

    public Object executeFunction(StrategyLogInfo strategyLogInfo,FunctionInfo functionInfo, Map<String, Object> objectMap, List<String> param_codes) throws ClassNotFoundException, IllegalAccessException, InstantiationException, ScriptException, NoSuchMethodException {
        String function_name = functionInfo.getFunction_name();
        String function_class = functionInfo.getFunction_class();
        String function_load_path = functionInfo.getFunction_load_path();
        String function_script = functionInfo.getFunction_script();

        if(!StringUtils.isEmpty(function_class)){
            String[] function_packages = function_class.split("\\.");
            String clsName = ArrayUtil.get(function_packages, function_packages.length-1);
            String clsInstanceName = StringUtils.uncapitalize(clsName);

            //加载三方工具类
            if(!StringUtils.isEmpty(function_load_path)){
                JarClassLoader jarClassLoader = JarClassLoader.loadJar(new File(function_load_path));
                Class cls = jarClassLoader.loadClass(function_class);
                Object clsInstance = cls.newInstance();
                objectMap.put(clsInstanceName, clsInstance);
                function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(param_codes, ",")+")";
                LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数: "+function_script+", 参数: "+JsonUtil.formatJsonString(objectMap));
                Object ret = GroovyFactory.execExpress(function_script, objectMap);
                return ret;
            }else{
                Object clsInstance = ClassLoaderUtil.loadClass(function_class).newInstance();
                objectMap.put(clsInstanceName, clsInstance);
                function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(param_codes, ",")+")";
                LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数: "+function_script+", 参数: "+JsonUtil.formatJsonString(objectMap));
                Object ret = GroovyFactory.execExpress(function_script, objectMap);
                return ret;
            }
        }else if(!StringUtils.isEmpty(function_script)){
            Map<String,Object> stringObjectMap = new LinkedHashMap<>();
            for (String param_code: param_codes){
                stringObjectMap.put(param_code, objectMap.get(param_code));
            }
            LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "函数: "+function_script+", 参数: "+JsonUtil.formatJsonString(stringObjectMap));
            Object ret = GroovyFactory.execExpress(function_script, function_name, stringObjectMap);
            return ret;
        }

        return null;
    }

    /**
     * 历史版本对比函数解析
     * 5.3.5版本及之后已废弃, 使用下发execReturnDiffExpress(StrategyLogInfo strategyLogInfo,Object ret, String return_operate_value, Map<String, Object> objectMap)函数代替
     * @param strategyLogInfo
     * @param ret
     * @param return_value_express
     * @param return_operate
     * @param return_operate_value
     * @param return_diff_enable
     * @param objectMap
     * @return
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    public Object execReturnDiffExpress(StrategyLogInfo strategyLogInfo,Object ret, String return_value_express, String return_operate, String return_operate_value, String return_diff_enable, Map<String, Object> objectMap) throws ScriptException, NoSuchMethodException {

        Map<String, Object> tmp = new HashMap<>();
        Jinjava jinjava=new Jinjava();
        String function_name = "plugin_function_if_v1";
        String str_pre="";
        String str_suffix="";



        //解析return_operate_value
        String new_return_operate_value = jinjava.render(return_operate_value, tmp);

        objectMap.put("ret", ret);
        if(!StringUtils.isEmpty(new_return_operate_value)){
            objectMap.put(new_return_operate_value, new_return_operate_value);
        }

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
            function_script = "return "+return_value_express;
        }

        tmp.put("return_value_express", return_value_express);//取值表达式

        function_script = jinjava.render(function_script, tmp);//替换可变参数

        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "结果对比函数: "+function_name+", "+function_script+", 参数: "+JsonUtil.formatJsonString(objectMap));

        Object obj = GroovyFactory.execExpress(function_script, objectMap);

        return obj;

    }

    /**
     * 执行 对比 表达式
     * @param strategyLogInfo
     * @param ret
     * @param return_operate_value
     * @param objectMap
     * @return
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    public Object execReturnDiffExpress(StrategyLogInfo strategyLogInfo,Object ret, String return_operate_value, Map<String, Object> objectMap) throws ScriptException, NoSuchMethodException {

        String function_name = "plugin_function_if_v0";

        objectMap.put("ret", ret);

        if(StringUtils.isEmpty(return_operate_value)){
            return false;
        }

        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "结果对比函数: "+function_name+", "+return_operate_value+", 参数: "+JsonUtil.formatJsonString(objectMap));

        Object obj = GroovyFactory.execExpress(return_operate_value, objectMap);
        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(),"结果: "+(obj!=null?obj.toString():"空"));
        return obj;

    }

    /**
     * 解析 取值表达式
     * @param strategyLogInfo
     * @param ret
     * @param return_value_value
     * @param objectMap
     * @return
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    public Object execFunctionExpress(StrategyLogInfo strategyLogInfo,Object ret, String return_value_value, Map<String, Object> objectMap) throws ScriptException, NoSuchMethodException {

        String function_name = "plugin_function_if_v0";

        objectMap.put("ret", ret);

        if(StringUtils.isEmpty(return_value_value)){
            return null;
        }

        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "取值结果函数: "+function_name+", "+return_value_value+", 参数: "+JsonUtil.formatJsonString(objectMap));

        Object obj = GroovyFactory.execExpress(return_value_value, objectMap);
        LogUtil.console(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(),"取值结果: "+(obj!=null?obj.toString():"空"));
        return obj;

    }

}
