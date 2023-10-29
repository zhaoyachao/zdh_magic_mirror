package com.zyc.plugin.impl;

import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.PluginInfo;
import com.zyc.plugin.PluginService;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class KafkaPluginServiceImpl implements PluginService {

    @Override
    public String execute(PluginInfo pluginInfo, List<Map> params, String rs) {
        try{
            System.out.println("用户: "+rs+" ,插件: "+pluginInfo.getPlugin_code()+",  参数: "+ JSON.toJSONString(params));
            Properties props = getParams(params);
            String topic = props.getProperty("topic","test");
            String msg = props.getProperty("message", "");
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, msg)).get();
            return "success";
        }catch (Exception e){

        }
        return "fail";
    }

    @Override
    public String execute(PluginInfo pluginInfo, List<Map> params, Set<String> rs) {
        return null;
    }

    public Properties getParams(List<Map> params){
        Properties props = new Properties();

        for (Map<String,Object> param: params){
            String key = param.get("param_code").toString();
            String value = param.getOrDefault("param_value", "").toString();
            if(!StringUtils.isEmpty(value)){
                props.put(key, value);
            }
        }
        return props;
    }
}
