package com.zyc.ship.engine.impl.executor;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.JsonUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class VarPoolExecutor extends BaseExecutor{

    private static Logger logger= LoggerFactory.getLogger(VarPoolExecutor.class);

    public ShipResult execute(StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        Map<String, Object> ext = shipEvent.getRunParam();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            Map<String, Object> jsonObject = JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());
            String varpool_param_str=jsonObject.get("varpool_param").toString();
            List<Map> varpool_params = JsonUtil.toJavaListBean(varpool_param_str, Map.class);

            Map<String, Object> varParams = new HashMap<>();
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
                        varParams.put(secondKey, varpool_value);
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                        if(ext.containsKey(secondKey)){
                            Object value = ext.get(secondKey);
                            if(value != null && value instanceof Collection){
                                ((Collection) value).addAll(set);
                                set = Sets.newHashSet(value);
                            }
                        }
                        varParams.put(secondKey, set);
                    }else if(varpool_operate.equalsIgnoreCase("concat")){
                        Object value = ext.get(secondKey);
                        if(value != null){
                            varpool_value = value.toString()+","+varpool_value;
                        }
                        varParams.put(secondKey, varpool_value);
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        throw new Exception("字符串类型,不支持KV追加操作");
                    }
                }else if(varpool_domain_type.equalsIgnoreCase("int") || varpool_domain_type.equalsIgnoreCase("decimal")){
                    if(varpool_operate.equalsIgnoreCase("eq")){
                        varParams.put(secondKey, varpool_value);
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                        if(ext.containsKey(secondKey)){
                            Object value = ext.get(secondKey);
                            if(value != null && value instanceof Collection){
                                ((Collection) value).addAll(set);
                                set = Sets.newHashSet(value);
                            }
                        }
                        varParams.put(secondKey, set);
                    }else if(varpool_operate.equalsIgnoreCase("concat")){
                        throw new Exception("数值类型,不支持拼接操作");
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        throw new Exception("数值类型,不支持KV追加操作");
                    }
                }else if(varpool_domain_type.equalsIgnoreCase("list")){
                    if(varpool_operate.equalsIgnoreCase("eq")){
                        List<Object> list = Lists.newArrayList(varpool_value.split(varpool_domain_sep));
                        varParams.put(secondKey, list);
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                        if(ext.containsKey(secondKey)){
                            Object value = ext.get(secondKey);
                            if(value != null && value instanceof Collection){
                                ((Collection) value).addAll(set);
                                set = Sets.newHashSet(value);
                            }
                        }
                        varParams.put(secondKey, set);
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
                        varParams.put(secondKey, map);
                    }else if(varpool_operate.equalsIgnoreCase("add")){
                        throw new Exception("字典类型,请选择KV追加操作");
                    }else if(varpool_operate.equalsIgnoreCase("concat")){
                        throw new Exception("字典类型,不支持拼接操作");
                    }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                        Map<String, String> map = parseKeyValuePairs(varpool_value, varpool_domain_sep);
                        if(ext.containsKey(secondKey)){
                            Object value = ext.get(secondKey);
                            if(value != null && value instanceof Map){
                                map.putAll((Map)value);
                                ((Map) value).putAll(map);
                                map = (Map)value;
                            }
                        }
                        varParams.put(secondKey, map);
                    }
                }else{
                    throw new Exception("不支持的数据类型");
                }
            }

            ext.putAll(varParams);

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
