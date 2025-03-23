package com.zyc.ship.engine.impl.executor.plugin;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.HttpUtil;
import com.zyc.common.util.JsonUtil;
import com.zyc.ship.disruptor.ShipEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HttpPlugin implements Plugin{

    private static Logger logger= LoggerFactory.getLogger(HttpPlugin.class);

    private String rule_id;
    private Map<String, Object> run_jsmind_data;
    private StrategyInstance strategyInstance;
    private ShipEvent shipEvent;

    public HttpPlugin(String rule_id, Map<String, Object> run_jsmind_data, StrategyInstance strategyInstance, ShipEvent shipEvent){
        this.rule_id = rule_id;
        this.run_jsmind_data = run_jsmind_data;
        this.strategyInstance = strategyInstance;
        this.shipEvent = shipEvent;
    }

    @Override
    public String getName() {
        return "http";
    }

    @Override
    public boolean execute() throws Exception {
        try{
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson((run_jsmind_data).get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

            Properties props = new Properties();

            for (Map<String,Object> param: rule_params){
                String key = param.get("param_code").toString();
                String value = param.getOrDefault("param_value", "").toString();
                if(!StringUtils.isEmpty(value)){
                    props.put(key, value);
                }
            }

            String method = props.getProperty("method","post");
            String request_params = props.getProperty("request_params", "");
            String url = props.getProperty("url", "");
            String data_type = props.getProperty("data_type", "");
            String proxy_url = props.getProperty("proxy_url", "");

            String return_param = props.getProperty("return_param", "");
            String return_param_value = props.getProperty("return_param_value", "");
            String return_value_type = props.getProperty("return_value_type", "json");

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
                    return true;
                }
            }

            return false;
        }catch (Exception e){
            logger.error("ship plugin http error: ", e);
            throw e;
        }
    }
}
