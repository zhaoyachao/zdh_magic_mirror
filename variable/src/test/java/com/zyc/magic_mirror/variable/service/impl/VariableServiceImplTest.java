package com.zyc.magic_mirror.variable.service.impl;


import cn.hutool.core.net.url.UrlBuilder;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
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
        JedisPoolUtil.redisClient().hSet(uid, sk, JsonUtil.formatJsonString(param));

        System.out.println(JedisPoolUtil.redisClient().hGet(uid, sk));
        Map<Object, Object> r = JedisPoolUtil.redisClient().hGetAll(uid);
        for(Object key: r.keySet()){
            System.out.println("key: "+key.toString());
            Map mp = JsonUtil.toJavaBean(r.get(key).toString(), Map.class);
            System.out.println(JsonUtil.formatJsonString(mp));
        }

    }

    @Test
    public void testJSON(){
        String str="{\"age\": 19, \"job\": \"IT\"}";

        Map map = JsonUtil.toJavaBean(str, Map.class);

    }

    @Test
    public void testUrl(){
        String url="http://127.0.0.1/api/v1/all?a1=b1";

        Map map = UrlBuilder.of(url).getQuery().getQueryMap();

        System.out.println(JsonUtil.formatJsonString(map));
    }


}
