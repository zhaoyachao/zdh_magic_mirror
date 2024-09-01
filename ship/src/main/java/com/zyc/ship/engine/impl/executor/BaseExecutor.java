package com.zyc.ship.engine.impl.executor;

import com.zyc.common.redis.JedisPoolUtil;

import java.util.HashMap;
import java.util.Map;

public class BaseExecutor {


    /**
     * 获取变量池信息,并合并到params集合中
     * @param logGroupId
     * @param params
     */
    public void mergeMapByVarPool(String logGroupId, Map<String, Object> params){
        Map<Object, Object> varPoolMap = getVarPoolBy(logGroupId);

        if(varPoolMap != null && varPoolMap.size() > 0){
            for(Map.Entry entry: varPoolMap.entrySet()){
                String key = entry.getKey().toString();
                Object value = entry.getKey();
                params.put(key, value);
            }
        }
    }


    public Map<Object, Object> getVarPoolBy(String logGroupId){
        String key = "varpool:logid:"+logGroupId;
        if(JedisPoolUtil.redisClient().isExists(key)){
            return JedisPoolUtil.redisClient().hGetAll(key);
        }
        return new HashMap<>();
    }

}
