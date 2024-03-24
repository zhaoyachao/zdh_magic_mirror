package com.zyc.variable.service.impl;


import com.alibaba.fastjson.JSONObject;
import com.zyc.common.redis.JedisPoolUtil;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class VariableServiceImplTest {
    @Test
    public void get() throws IOException {

        Properties properties = new Properties();
        InputStream inputStream= VariableServiceImplTest.class.getClassLoader().getResourceAsStream("application.properties");
        properties.load(inputStream);
        JedisPoolUtil.connect(properties);

        String uid = "uid_zyc";
        String sk = "tag_age";
        Map<String,String> param = new HashMap<>();
        param.put("age", "20");
        JedisPoolUtil.redisClient().hSet(uid, sk, JSONObject.toJSONString(param));

        System.out.println(JedisPoolUtil.redisClient().hGet(uid, sk));
        Map<Object, Object> r = JedisPoolUtil.redisClient().hGetAll(uid);
        for(Object key: r.keySet()){
            System.out.println("key: "+key.toString());
            Map mp = JSONObject.parseObject(r.get(key).toString(), Map.class);
            System.out.println(JSONObject.toJSONString(mp));
        }

    }
}
