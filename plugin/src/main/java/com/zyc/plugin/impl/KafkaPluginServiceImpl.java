package com.zyc.plugin.impl;

import com.zyc.common.entity.DataPipe;
import com.zyc.common.entity.PluginInfo;
import com.zyc.common.plugin.PluginParam;
import com.zyc.common.plugin.PluginResult;
import com.zyc.common.plugin.PluginService;
import com.zyc.common.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class KafkaPluginServiceImpl implements PluginService {

    @Override
    public PluginResult execute(PluginInfo pluginInfo, PluginParam pluginParam, DataPipe rs, Map<String,Object> params) {
        KafkaPluginResult kafkaPluginResult = new KafkaPluginResult();
        KafkaProducer<String, String> producer=null;
        try{
            System.out.println("用户: "+rs.getUdata()+" ,插件: "+pluginInfo.getPlugin_code()+",  参数: "+ JsonUtil.formatJsonString(pluginParam));
            Properties props = getParams(pluginParam);

            if (!props.containsKey("bootstrap.servers")) {
                props.put("bootstrap.servers", props.getProperty("zk_url"));
            }
            if (!props.containsKey("key.serializer")) {
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }
            if (!props.containsKey("value.serializer")) {
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }
            if (!props.containsKey("request.timeout.ms")) {
                props.put("request.timeout.ms", "1000");
            }
            if (!props.containsKey("timeout.ms")) {
                props.put("timeout.ms", "1000");
            }
            if (!props.containsKey("max.block.ms")) {
                //send发送时最大阻塞时间
                props.put("max.block.ms", "1000");
            }
            if (!props.containsKey("retries")) {
                //send发送时最大阻塞时间
                props.put("retries", "10");
            }

            String topic = props.getProperty("topic","test");
            String msg = props.getProperty("message", "");
            producer = new KafkaProducer<>(props);
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, msg)).get();
            kafkaPluginResult.setCode(0);
            kafkaPluginResult.setMessage("success");
            kafkaPluginResult.setResult(JsonUtil.formatJsonString(recordMetadata));
            return kafkaPluginResult;
        }catch (Exception e){
            e.printStackTrace();
            kafkaPluginResult.setCode(-1);
            kafkaPluginResult.setMessage(e.getMessage());
        }finally {
            if(producer != null){
                producer.close();
            }
        }
        return kafkaPluginResult;
    }

    @Override
    public PluginParam getPluginParam(Object param) {
        return new KafkaPluginParam((List<Map>)param);
    }

    public Properties getParams(PluginParam pluginParam){
        KafkaPluginParam kafkaPluginParam = (KafkaPluginParam)pluginParam;
        Properties props = new Properties();

        for (Map<String,Object> param: kafkaPluginParam.getParams()){
            String key = param.get("param_code").toString();
            String value = param.getOrDefault("param_value", "").toString();
            if(!StringUtils.isEmpty(value)){
                props.put(key, value);
            }
        }
        return props;
    }

    public static class KafkaPluginParam implements PluginParam{

        public KafkaPluginParam(List<Map> params) {
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

    public static class KafkaPluginResult implements PluginResult{

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
