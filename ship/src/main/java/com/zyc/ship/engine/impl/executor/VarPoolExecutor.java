package com.zyc.ship.engine.impl.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
            String key = "varpool:reqid:"+shipEvent.getRequestId()+":"+shipEvent.getStrategyGroupInstanceId();
            for (Map varpool: varpool_params){
                String varpool_code = varpool.getOrDefault("varpool_code","").toString();
                String varpool_operate = varpool.getOrDefault("varpool_domain_operate","eq").toString();
                String varpool_domain = varpool.getOrDefault("varpool_domain","").toString();
                String varpool_value = varpool.getOrDefault("varpool_domain_value","").toString();
                String varpool_domain_type = varpool.getOrDefault("varpool_domain_type","").toString();
                String varpool_domain_sep = varpool.getOrDefault("varpool_domain_sep",";").toString();
                String secondKey = varpool_domain+":"+varpool_code;

                if(varpool_domain_type.equalsIgnoreCase("string")){
                    if(varpool_operate.equalsIgnoreCase("eq")){
                        JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
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
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        throw new Exception("字符串类型,不支持KV追加操作");
                    }
                }else if(varpool_domain_type.equalsIgnoreCase("int") || varpool_domain_type.equalsIgnoreCase("decimal")){
                    if(varpool_operate.equalsIgnoreCase("eq")){
                        JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                        Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                        if(value != null){
                            Set set2 = JSON.parseObject(value.toString(), Set.class);
                            set2.add(set);
                            set = set2;
                        }
                        JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(set));
                    }else if(varpool_operate.equalsIgnoreCase("concat")){
                        throw new Exception("数值类型,不支持拼接操作");
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        throw new Exception("数值类型,不支持KV追加操作");
                    }
                }else if(varpool_domain_type.equalsIgnoreCase("list")){
                    if(varpool_operate.equalsIgnoreCase("eq")){
                        List<Object> list = Lists.newArrayList(varpool_value.split(varpool_domain_sep));
                        JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(list));
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                        Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                        if(value != null){
                            Set set2 = JSON.parseObject(value.toString(), Set.class);
                            set2.addAll(set);
                            set = set2;
                        }
                        JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(set));
                    }else if(varpool_operate.equalsIgnoreCase("concat")){
                        throw new Exception("集合类型,不支持拼接操作");
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        throw new Exception("集合类型,不支持KV追加操作");
                    }
                }else if(varpool_domain_type.equalsIgnoreCase("map")){
                    if(!varpool_value.contains("=")){
                        throw new Exception("字典格式 k1=v1;k2=v2");
                    }
                    if(varpool_operate.equalsIgnoreCase("eq")){
                        Map<String, String> map = parseKeyValuePairs(varpool_value, varpool_domain_sep);
                        JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(map));
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        throw new Exception("字典类型,请选择KV追加操作");
                    }else if(varpool_operate.equalsIgnoreCase("concat")){
                        throw new Exception("字典类型,不支持拼接操作");
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        Map<String, String> map = parseKeyValuePairs(varpool_value, varpool_domain_sep);
                        Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                        if(value != null){
                            Map<String,String> old = JSON.parseObject(value.toString(), Map.class);
                            old.putAll(map);
                            map = old;
                        }
                        JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(map));
                    }
                }else{
                    throw new Exception("不支持的数据类型");
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

    public static Map<String, String> parseKeyValuePairs(String input, String sep) {
        Map<String, String> map = new HashMap<>();
        if (input != null && !input.isEmpty()) {
            String[] pairs = input.split(sep);
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    map.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return map;
    }
}
