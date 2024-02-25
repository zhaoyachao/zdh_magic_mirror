package com.zyc.ship.util;

import com.alibaba.fastjson.JSON;
import com.zyc.common.util.HttpClientUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LabelHttpUtil {

    private static String url;

    public static void init(Properties properties){
        LabelHttpUtil.url = properties.getProperty("label.http.url");
    }

    public static Map<String,Object> post(String body){
        try{

            Map<String,Object> result = JSON.parseObject(HttpClientUtil.postJson(url, body),Map.class);
            return result;
        }catch (Exception e){
            e.printStackTrace();
        }
        return new HashMap<>();
    }

}
