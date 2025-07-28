package com.zyc.magic_mirror.ship.engine.impl.executor;

import cn.hutool.core.util.ArrayUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.FunctionInfo;
import com.zyc.magic_mirror.common.groovy.GroovyFactory;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import com.zyc.magic_mirror.ship.service.impl.CacheFunctionServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.*;

public class FunctionExecutor extends BaseExecutor{
    private Logger logger= LoggerFactory.getLogger(this.getClass());

    public ShipResult execute(ShipEvent shipEvent, Map<String, Object> run_jsmind_data, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String function_name = run_jsmind_data.get("rule_id").toString();
            String return_diff_enable = run_jsmind_data.getOrDefault("return_diff_enable", "false").toString();
            String return_value_express = run_jsmind_data.getOrDefault("return_value_express", "ret").toString();
            String return_value_type = run_jsmind_data.getOrDefault("return_value_type", "string").toString();
            String return_operate = run_jsmind_data.getOrDefault("return_operate", "").toString();
            String return_operate_value = run_jsmind_data.getOrDefault("return_operate_value", "").toString();
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

            CacheFunctionServiceImpl cacheFunctionService = new CacheFunctionServiceImpl();
            FunctionInfo functionInfo = cacheFunctionService.selectByFunctionCode(function_name);

            Jinjava jinjava=new Jinjava();
            Map<String, Object> objectMap = new HashMap<>();
            objectMap.putAll(shipEvent.getRunParam());

            List<String> param_values = new ArrayList<>();
            for(Map map: rule_params){
                String value = map.get("param_value").toString();
                param_values.add(value);
                String param_code = map.get("param_code").toString();
                String param_value = map.get("param_value").toString();
                String param_type = map.getOrDefault("param_type", "").toString();
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
            }

            Object res = functionExcute(functionInfo, objectMap);

            shipResult.addObj2Map("ret", res);
            //在线模块尽量直接使用返回结果, 需要设置 开启对比：关闭,取值表达式：为空或者ret
            if(return_diff_enable.equalsIgnoreCase("false") && (StringUtils.isEmpty(return_value_express) || return_value_express.equalsIgnoreCase("ret"))){
                if(res == null){
                    tmp = ShipResultStatusEnum.ERROR.code;
                }else{
                    tmp = ShipResultStatusEnum.SUCCESS.code;
                }
                shipResult.setStatus(tmp);
                return shipResult;
            }

            //如果开启对比: 开启对比：关闭, 取值表达式不为空且不为ret, 此时需要通过解析表达式获取结果
            if(return_diff_enable.equalsIgnoreCase("false") && !StringUtils.isEmpty(return_value_express) && !return_value_express.equalsIgnoreCase("ret")){
                objectMap.put("uid", uid);//获取当前结果集信息
                objectMap.put("udata", uid);//获取当前结果集信息
                Object ret_express_value = execReturnDiffExpress(res, return_value_express, return_operate, return_operate_value, return_diff_enable, objectMap);
                if(ret_express_value == null){
                    tmp = ShipResultStatusEnum.ERROR.code;
                }else{
                    tmp = ShipResultStatusEnum.SUCCESS.code;
                }
                shipResult.addObj2Map("ret_express_value", ret_express_value);
                shipResult.setStatus(tmp);
                return shipResult;
            }

            if(return_diff_enable.equalsIgnoreCase("true")) {
                //如果开启对比
                Object ret_express_value = execReturnDiffExpress(res, return_value_express, return_operate, return_operate_value, return_diff_enable, objectMap);
                //开启对比,结果为true表示成功数据,false为失败数据
                if (ret_express_value.toString().equalsIgnoreCase("true")) {
                    tmp = ShipResultStatusEnum.SUCCESS.code;
                }else{
                    tmp = ShipResultStatusEnum.ERROR.code;
                }
                shipResult.addObj2Map("ret_diff_value", ret_express_value);
                shipResult.setStatus(tmp);
                return shipResult;
            }

        }catch (Exception e){
            logger.error("ship excutor function error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }

    public Object functionExcute(FunctionInfo functionInfo, Map<String, Object> objectMap){
        try{
            String function_name = functionInfo.getFunction_name();
            String function_class = functionInfo.getFunction_class();
            String function_load_path = functionInfo.getFunction_load_path();
            String function_script = functionInfo.getFunction_script();
            List<Object> jsonArray = functionInfo.getParam_json_object();

            List<String> params = new ArrayList<>();
            for(int i=0;i<jsonArray.size();i++){
                String param_code = ((Map<String, Object>)jsonArray.get(i)).get("param_code").toString();
                params.add(param_code);
            }

            if(CacheFunctionServiceImpl.cacheFunctionInstance.containsKey(function_name)){
                Object clsInstance = CacheFunctionServiceImpl.cacheFunctionInstance.get(function_name);
                if(!StringUtils.isEmpty(function_class)){
                    String[] function_packages = function_class.split("\\.");
                    String clsName = ArrayUtil.get(function_packages, function_packages.length-1);
                    String clsInstanceName = StringUtils.uncapitalize(clsName);
                    //加载三方工具类
                    if(!StringUtils.isEmpty(function_load_path)){
                        objectMap.put(clsInstanceName, clsInstance);
                        function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(params, ",")+")";
                        Object ret = GroovyFactory.execExpress(function_script, objectMap, true);
                        return ret;
                    }else{
                        objectMap.put(clsInstanceName, clsInstance);
                        function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(params, ",")+")";
                        Object ret = GroovyFactory.execExpress(function_script, objectMap, true);
                        return ret;
                    }
                }
            }
            if(!StringUtils.isEmpty(function_script)){
                Object ret = GroovyFactory.execExpress(function_script, function_name, objectMap);
                return ret;
            }
        }catch (Exception e){
            logger.error("ship excutor functionExecutor error: ", e);
        }
        return null;
    }

    public Object execReturnDiffExpress(Object ret, String return_value_express, String return_operate, String return_operate_value, String return_diff_enable, Map<String, Object> objectMap) throws ScriptException, NoSuchMethodException {

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

        logger.info("结果对比函数: "+function_name+", "+function_script+", 参数: "+ JsonUtil.formatJsonString(objectMap));

        Object obj = GroovyFactory.execExpress(function_script, objectMap);

        return obj;

    }


}
