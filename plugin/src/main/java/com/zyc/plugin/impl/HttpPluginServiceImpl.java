package com.zyc.plugin.impl;

import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.PluginInfo;
import com.zyc.common.plugin.PluginParam;
import com.zyc.common.plugin.PluginResult;
import com.zyc.common.plugin.PluginService;
import com.zyc.common.util.HttpUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class HttpPluginServiceImpl implements PluginService {

    @Override
    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, String rs) {
        HttpPluginResult httpPluginResult = new HttpPluginResult();
        try{
            System.out.println("用户: "+rs+" ,插件: "+pluginInfo.getPlugin_code()+",  参数: "+ JSON.toJSONString(pluginParam));
            Properties props = getParams(pluginParam);
            String method = props.getProperty("method","post");
            String request_params = props.getProperty("request_params", "");
            String url = props.getProperty("url", "");
            String data_type = props.getProperty("data_type", "");

            String res = "";
            if(method.equalsIgnoreCase("post")){
                if(!data_type.equalsIgnoreCase("json")){
                    throw new Exception("当请求类型为post时数据类型仅支持json");
                }

                res = HttpUtil.postJSON(url, request_params);
            }else if(method.equalsIgnoreCase("get")){
                List<NameValuePair> npl=new ArrayList<>();
                res = HttpUtil.getRequest(url+"?"+request_params, npl);
            }else{
                throw new Exception("仅支持get,post请求");
            }

            httpPluginResult.setCode(0);
            httpPluginResult.setMessage("success");
            httpPluginResult.setResult(res);

            return httpPluginResult;
        }catch (Exception e){
            httpPluginResult.setCode(-1);
            httpPluginResult.setMessage(e.getMessage());
        }
        return httpPluginResult;
    }

    @Override
    public PluginParam getPluginParam(Object param) {
        return new HttpPluginParam((List<Map>)param);
    }

    public Properties getParams(PluginParam pluginParam){
        HttpPluginParam httpPluginParam = (HttpPluginParam)pluginParam;
        Properties props = new Properties();

        for (Map<String,Object> param: httpPluginParam.getParams()){
            String key = param.get("param_code").toString();
            String value = param.getOrDefault("param_value", "").toString();
            if(!StringUtils.isEmpty(value)){
                props.put(key, value);
            }
        }
        return props;
    }

    public static class HttpPluginParam implements PluginParam{

        public HttpPluginParam(List<Map> params) {
            this.params = params;
        }

        public List<Map> params;

        public List<Map> getParams() {
            return params;
        }

        public void setParams(List<Map> params) {
            this.params = params;
        }
    }

    public static class HttpPluginResult implements PluginResult{

        private int code;

        private Object result;

        private String message;

        @Override
        public int getCode() {
            return this.code;
        }

        @Override
        public Object getResult() {
            return this.result;
        }

        @Override
        public String getMessage() {
            return this.message;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public void setResult(Object result) {
            this.result = result;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

}
