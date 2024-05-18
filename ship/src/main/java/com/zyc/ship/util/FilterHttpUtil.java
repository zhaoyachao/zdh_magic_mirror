package com.zyc.ship.util;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FilterHttpUtil {

    private static Logger logger= LoggerFactory.getLogger(FilterHttpUtil.class);

    private static String url;

    public static void init(Properties properties){
        FilterHttpUtil.url = properties.getProperty("filter.http.url");
    }

    public static Map<String,Object> post(String body){
        try{
            Map<String,Object> result = JSON.parseObject(HttpUtil.post(url, body),Map.class);
            return result;
        }catch (Exception e){
            logger.error("ship server filterpost error: ", e);
        }
        return new HashMap<>();
    }

}
