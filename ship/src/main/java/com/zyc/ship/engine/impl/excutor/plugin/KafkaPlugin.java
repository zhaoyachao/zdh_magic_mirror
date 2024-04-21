package com.zyc.ship.engine.impl.excutor.plugin;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.StrategyInstance;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaPlugin implements Plugin{


    private String rule_id;
    private Object run_jsmind_data;
    private StrategyInstance strategyInstance;

    public KafkaPlugin(String rule_id, Object run_jsmind_data, StrategyInstance strategyInstance){
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

            if (!props.containsKey("bootstrap.servers")) {
                props.put("bootstrap.servers", props.getProperty("zk_url"));
            }
            if (!props.containsKey("key.serializer")) {
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }
            if (!props.containsKey("value.serializer")) {
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            }

            String topic = props.getProperty("topic","test");
            String msg = props.getProperty("message", "");
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            producer.send(new ProducerRecord<>(topic, msg));
            return true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
