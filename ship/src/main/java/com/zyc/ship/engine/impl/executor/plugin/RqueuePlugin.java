package com.zyc.ship.engine.impl.executor.plugin;

import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import com.zyc.rqueue.RQueueMode;
import com.zyc.ship.disruptor.ShipEvent;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 分布式队列
 * 支持延迟,优先级,普通队列
 */
public class RqueuePlugin implements Plugin{

    private static Logger logger= LoggerFactory.getLogger(RqueuePlugin.class);

    private String rule_id;
    private Object run_jsmind_data;
    private StrategyInstance strategyInstance;
    private ShipEvent shipEvent;

    public RqueuePlugin(String rule_id, Object run_jsmind_data, StrategyInstance strategyInstance, ShipEvent shipEvent){
        this.rule_id = rule_id;
        this.run_jsmind_data = run_jsmind_data;
        this.strategyInstance = strategyInstance;
        this.shipEvent = shipEvent;
    }

    @Override
    public String getName() {
        return "rqueue";
    }

    @Override
    public boolean execute() throws Exception {
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

            String queue_url = props.getProperty("queue_url", "");
            String queue_password = props.getProperty("queue_password", "");
            String queue_type = props.getProperty("queue_type","block");
            String queue_key = props.getProperty("queue_key", "");
            String priority = props.getProperty("priority", "5");
            String delay = props.getProperty("delay", "120");


            if(Lists.newArrayList("block","priority","delay").contains(queue_type)){
                throw new Exception("队列类型不满足,仅支持block, priority, delay");
            }

            if(queue_type.equalsIgnoreCase("priority")){
                if(priority.equalsIgnoreCase("") || !NumberUtil.isInteger(priority)){
                    throw new Exception("优先级队列priority参数格式错误");
                }
            }

            if(queue_type.equalsIgnoreCase("delay")){
                if(priority.equalsIgnoreCase("") || !NumberUtil.isInteger(delay)){
                    throw new Exception("延迟队列delay参数格式错误");
                }
            }

            RQueueClient rQueueClient = null;
            if(StringUtils.isEmpty(queue_url)){
                rQueueClient = getQueueByType(queue_type, queue_key, null);
            }else{
                RedissonClient redissonClient = RQueueManager.buildRedissonClient(queue_url, queue_password);
                rQueueClient = getQueueByType(queue_type, queue_key, redissonClient);
            }

            if(queue_type.equalsIgnoreCase("priority")){
                rQueueClient.offer(JSONObject.toJSONString(shipEvent), NumberUtil.parseInt(priority));
            }

            if(queue_type.equalsIgnoreCase("delay")){
                rQueueClient.offer(JSONObject.toJSONString(shipEvent), NumberUtil.parseLong(delay), TimeUnit.SECONDS);
            }

            if(queue_type.equalsIgnoreCase("block")){
                rQueueClient.add(JSONObject.toJSONString(shipEvent));
            }

            return true;
        }catch (Exception e){
            logger.error("ship plugin http error: ", e);
            throw e;
        }
    }

    private RQueueClient getQueueByType(String queue_type, String queue_key, RedissonClient redissonClient) throws Exception {
        if(queue_type.equalsIgnoreCase("block")){
            return RQueueManager.getRQueueClient(queue_key);
        }

        if(queue_type.equalsIgnoreCase("priority")){
            return RQueueManager.getRQueueClient(queue_key, RQueueMode.PRIORITYQUEUE);
        }

        if(queue_type.equalsIgnoreCase("delay")){
            return RQueueManager.getRQueueClient(queue_key, RQueueMode.DELAYEDQUEUE);
        }

        throw new Exception("无法找到分布式队列");
    }
}
