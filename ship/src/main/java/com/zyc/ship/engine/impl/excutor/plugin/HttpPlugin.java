package com.zyc.ship.engine.impl.excutor.plugin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.HttpUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HttpPlugin implements Plugin{


    private String rule_id;
    private Object run_jsmind_data;
    private StrategyInstance strategyInstance;

    public HttpPlugin(String rule_id, Object run_jsmind_data, StrategyInstance strategyInstance){
        this.rule_id = rule_id;
        this.run_jsmind_data = run_jsmind_data;
        this.strategyInstance = strategyInstance;
    }

    @Override
    public boolean execute() {
        try{
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(((JSONObject)run_jsmind_data).get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

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

            String return_param = props.getProperty("return_param", "");
            String return_param_value = props.getProperty("return_param_value", "");
            String return_value_type = props.getProperty("return_value_type", "json");

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

            if(return_value_type.equalsIgnoreCase("json")){
                JSONObject jsonObject = JSON.parseObject(res);
                if(jsonObject.getString(return_param).equalsIgnoreCase(return_param_value)){
                    return true;
                }
            }

            return false;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
