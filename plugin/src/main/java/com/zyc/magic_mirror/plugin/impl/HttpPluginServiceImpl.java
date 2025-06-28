package com.zyc.magic_mirror.plugin.impl;

import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.PluginInfo;
import com.zyc.magic_mirror.common.plugin.PluginParam;
import com.zyc.magic_mirror.common.plugin.PluginResult;
import com.zyc.magic_mirror.common.plugin.PluginService;
import com.zyc.magic_mirror.common.util.HttpUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class HttpPluginServiceImpl implements PluginService {
    private static Logger logger= LoggerFactory.getLogger(HttpPluginServiceImpl.class);
    @Override
    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, DataPipe rs, Map<String,Object> params) {
        HttpPluginResult httpPluginResult = new HttpPluginResult();
        try{
            //logger.info("用户: "+rs.getUdata()+" ,插件: "+pluginInfo.getPlugin_code()+",  参数: "+ JsonUtil.formatJsonString(pluginParam));
            Properties props = getParams(pluginParam);
            String method = props.getProperty("method","post");
            String request_params = props.getProperty("request_params", "");
            String url = props.getProperty("url", "");
            String data_type = props.getProperty("data_type", "");
            String proxy_url = props.getProperty("proxy_url", "");

            String return_param = props.getProperty("return_param", "");
            String return_param_value = props.getProperty("return_param_value", "");
            String return_value_type = props.getProperty("return_value_type", "json");


            //url, request_params 增加动态信息转换
            Jinjava jinjava = new Jinjava();
            url = jinjava.render(url, params);
            request_params = jinjava.render(request_params, params);

            HttpHost proxy = null;
            if(!StringUtils.isEmpty(proxy_url)){
                proxy = HttpHost.create(proxy_url);
            }

            String res = "";
            if(method.equalsIgnoreCase("post")){
                if(!data_type.equalsIgnoreCase("json")){
                    throw new Exception("当请求类型为post时数据类型仅支持json");
                }

                res = HttpUtil.postJSON(url, request_params, proxy);
            }else if(method.equalsIgnoreCase("get")){
                List<NameValuePair> npl=new ArrayList<>();
                res = HttpUtil.getRequest(url+"?"+request_params, npl, proxy);
            }else{
                throw new Exception("仅支持get,post请求");
            }

            if(return_value_type.equalsIgnoreCase("json")){
                Map<String, Object> jsonObject = JsonUtil.toJavaMap(res);
                if(jsonObject.getOrDefault(return_param,"").toString().equalsIgnoreCase(return_param_value)){
                    throw new Exception(res);
                }
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

    /**
     * 批量处理待实现
     * @param pluginInfo
     * @param pluginParam
     * @param rs
     * @param params
     * @return
     */
    @Override
    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, Set<DataPipe> rs, Map<String, Object> params) {
        return null;
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

        private Set<DataPipe> batchResult;

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
        public Set<DataPipe> getBatchResult() {
            return batchResult;
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

        public void setBatchResult(Set<DataPipe> batchResult) {
            this.batchResult = batchResult;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
