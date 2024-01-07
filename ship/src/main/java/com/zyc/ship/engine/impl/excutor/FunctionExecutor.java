package com.zyc.ship.engine.impl.excutor;

import cn.hutool.core.util.ArrayUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.FunctionInfo;
import com.zyc.common.groovy.GroovyFactory;
import com.zyc.ship.service.impl.CacheFunctionServiceImpl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FunctionExecutor {

    public String execute(JSONObject run_jsmind_data){
        String tmp = "error";
        try{
            String function_name = run_jsmind_data.getString("rule_id");
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

            CacheFunctionServiceImpl cacheFunctionService = new CacheFunctionServiceImpl();
            FunctionInfo functionInfo = cacheFunctionService.selectByFunctionCode(function_name);

            List<String> param_value = new ArrayList<>();
            for(Map map: rule_params){
                String value = map.get("param_value").toString();
                param_value.add(value);
            }
            Object res = functionExcute(functionInfo, param_value.toArray(new String[param_value.size()]));
            if(res == null){
                tmp = "error";
            }else{
                tmp = "success";
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return tmp;
    }

    public Object functionExcute(FunctionInfo functionInfo, String[] param_value){
        try{
            String function_name = functionInfo.getFunction_name();
            String function_class = functionInfo.getFunction_class();
            String function_load_path = functionInfo.getFunction_load_path();
            String function_script = functionInfo.getFunction_script();
            JSONArray jsonArray = functionInfo.getParam_json_object();

            Map<String, Object> objectMap = new LinkedHashMap<>();
            List<String> params = new ArrayList<>();
            for(int i=0;i<jsonArray.size();i++){
                String param_code = jsonArray.getJSONObject(i).getString("param_code");
                objectMap.put(param_code, param_value[i]);
                params.add(param_code);
            }

            if(CacheFunctionServiceImpl.cacheFunctionInstance.containsKey(function_name)){
                Object clsInstance = CacheFunctionServiceImpl.cacheFunctionInstance.get(function_name);
                if(!StringUtils.isEmpty(function_class)){
                    String[] function_packages = function_class.split(",");
                    String clsName = ArrayUtil.get(function_packages, function_packages.length-1);
                    String clsInstanceName = StringUtils.uncapitalize(clsName);
                    //加载三方工具类
                    if(!StringUtils.isEmpty(function_load_path)){
                        objectMap.put(clsInstanceName, clsInstance);
                        function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(params, ",")+")";
                        Object ret = GroovyFactory.execExpress(function_script, objectMap);
                        return ret;
                    }else{
                        objectMap.put(clsInstanceName, clsInstance);
                        function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(params, ",")+")";
                        Object ret = GroovyFactory.execExpress(function_script, objectMap);
                        return ret;
                    }
                }
            }
            if(!StringUtils.isEmpty(function_script)){
                Object ret = GroovyFactory.execExpress(function_script, function_name, objectMap);
                return ret;
            }
        }catch (Exception e){

        }
        return null;
    }
}
