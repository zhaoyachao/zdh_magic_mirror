package com.zyc.ship.engine.impl.executor.plugin;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.StrategyInstance;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Administrator
 */
public class KafkaPlugin implements Plugin{

    private static Logger logger= LoggerFactory.getLogger(KafkaPlugin.class);
    private String rule_id;
    private Object run_jsmind_data;
    private StrategyInstance strategyInstance;

    public KafkaPlugin(String rule_id, Object run_jsmind_data, StrategyInstance strategyInstance){
        this.rule_id = rule_id;
        this.run_jsmind_data = run_jsmind_data;
        this.strategyInstance = strategyInstance;
    }

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public boolean execute() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer=null;
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
            Future<RecordMetadata> send = producer.send(new ProducerRecord<>(topic, msg));
            send.get().offset();
            return true;
        }catch (Exception e){
            logger.error("ship plugin kafka error: ", e);
            throw e;
        }finally {
            if(producer != null){
                producer.close();
            }
        }
    }
}
