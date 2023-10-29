package com.zyc.ship.util;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FilterHttpUtil {

    private static String url;

    public static void init(Properties properties){
        FilterHttpUtil.url = properties.getProperty("filter.http.url");
    }

    public static Map<String,Object> post(String body){
        try{
            Map<String,Object> result = JSON.parseObject(HttpUtil.post(url, body),Map.class);
            return result;
        }catch (Exception e){
            e.printStackTrace();
        }
        return new HashMap<>();
    }

}
