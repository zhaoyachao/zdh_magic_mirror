package com.zyc.variable.service.impl;


import com.alibaba.fastjson.JSONObject;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.variable.service.VariableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 变量查询
 */
public class VariableServiceImpl implements VariableService {

    private static Logger logger= LoggerFactory.getLogger(VariableServiceImpl.class);

    @Override
    public Object get(String product_code, String uid, String variable) {
        String key = product_code+"_tag_"+uid;
        Object o = JedisPoolUtil.redisClient().hGet(key, variable);
        if(o == null){
            //返回默认值
            return CacheLabelServiceImpl.cache.get(variable);
        }
        return o;
    }

    /**
     * 返回的map结构, key: 标签code, value: json字符串,且json的key是参数code,value:是参数值
     * example: {key: "tag_age", value: "{'age': '20'}"}
     * @param uid
     * @return
     */
    @Override
    public Map<String,String> getAll(String product_code,String uid) {
        try{
            String key = product_code+"_tag_"+uid;
            //string, string
            Map<String,String> result = new HashMap<>();
            //Map<String,Object>, key: label_code, value: 字符串类型,Map结构的参数及值
            Map<Object,Object> allVariable = JedisPoolUtil.redisClient().hGetAll(key);

            for (Map.Entry<Object,Object> entry:allVariable.entrySet()){
                result.put(entry.getKey().toString(), entry.getValue().toString());//必须存储字符串
            }
            //校验是有不存在的变量,附默认值
            for(Map.Entry<String,Map<String,Object>> entry: CacheLabelServiceImpl.cache.entrySet()){
                String variable = entry.getKey();
                if(!allVariable.containsKey(variable)){
                    result.put(variable, JSONObject.toJSONString(entry.getValue()));
                }
            }
            return result;
        }catch (Exception e){
            logger.error("variable variable getAll error: ", e);
        }
        return null;
    }

    @Override
    public Map<String, String> getMul(String product_code, List<String> variables, String uid) {
        try{
            String key = product_code+"_tag_"+uid;
            //string, string
            Map<String,String> result = new HashMap<>();
            for(String variable: variables){
                Object o = JedisPoolUtil.redisClient().hGet(key, variable);
                if(o == null){
                    o = CacheLabelServiceImpl.cache.get(variable);
                }
                result.put(variable, JSONObject.toJSONString(o));
            }
            return result;
        }catch (Exception e){
            logger.error("variable variable getMul error: ", e);
        }
        return null;
    }
}
