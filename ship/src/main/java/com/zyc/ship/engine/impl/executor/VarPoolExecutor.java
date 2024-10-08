package com.zyc.ship.engine.impl.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class VarPoolExecutor extends BaseExecutor{

    private static Logger logger= LoggerFactory.getLogger(VarPoolExecutor.class);

    public ShipResult execute(StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            JSONObject jsonObject = JSON.parseObject(strategyInstance.getRun_jsmind_data());
            String varpool_param_str=jsonObject.get("varpool_param").toString();
            List<Map> varpool_params = JSON.parseArray(varpool_param_str, Map.class);

            //写入变量池
            String key = "varpool:logid:"+shipEvent.getLogGroupId();
            for (Map varpool: varpool_params){
                String varpool_code = varpool.getOrDefault("varpool_code","").toString();
                String varpool_operate = varpool.getOrDefault("varpool_domain_operate","eq").toString();
                String varpool_domain = varpool.getOrDefault("varpool_domain","").toString();
                String varpool_value = varpool.getOrDefault("varpool_domain_value","").toString();
                String secondKey = varpool_domain+":"+varpool_code;

                if(varpool_operate.equalsIgnoreCase("eq")){
                    JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                }else if(varpool_operate.equalsIgnoreCase("add")){
                    Set set = Sets.newHashSet(varpool_value);
                    Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                    if(value != null){
                        set = JSON.parseObject(value.toString(), Set.class);
                        set.add(varpool_value);
                    }
                    JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(set));
                }else if(varpool_operate.equalsIgnoreCase("concat")){
                    Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                    if(value != null){
                        varpool_value = value.toString()+","+varpool_value;
                    }
                    JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                }else if(varpool_operate.equalsIgnoreCase("put")){
                    JSONObject jsonObject1 = new JSONObject();
                    Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                    if(value != null){
                        jsonObject1 = JSON.parseObject(value.toString());
                    }
                    jsonObject1.put(varpool_value.split(";")[0], varpool_value.split(";")[1]);
                    JedisPoolUtil.redisClient().hSet(key, secondKey,jsonObject1.toJSONString());
                }
            }

            //缓存5分钟
            if(JedisPoolUtil.redisClient().isExists(key)){
                JedisPoolUtil.redisClient().expire(key, 60*5L);
            }

        }catch (Exception e){
            logger.error("ship excutor shunt error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
