package com.zyc.magic_mirror.variable.service.impl;

import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.variable.service.FilterService;

import java.util.HashMap;
import java.util.Map;

public class FilterServiceImpl implements FilterService {

    @Override
    public boolean isHit(String uid, String filter_code) {
        String key = "filter_"+uid;
        Object o = JedisPoolUtil.redisClient().hGet(key, filter_code);
        if(o == null){
            //返回默认值
            return false;
        }
        return true;
    }

    @Override
    public  Map<String,String> getFilter(String uid) {
        String key = "filter_"+uid;
        Map<String,String> result = new HashMap<>();
        Map<Object,Object> allFilter = JedisPoolUtil.redisClient().hGetAll(key);
        for (Map.Entry<Object,Object> entry:allFilter.entrySet()){
            result.put(entry.getKey().toString(), entry.getValue().toString());//必须存储字符串
        }
        return result;
    }


}
