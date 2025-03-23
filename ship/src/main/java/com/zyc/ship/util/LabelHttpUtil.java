package com.zyc.ship.util;

import com.zyc.common.util.HttpClientUtil;
import com.zyc.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LabelHttpUtil {

    private static Logger logger= LoggerFactory.getLogger(LabelHttpUtil.class);

    private static String url;

    public static void init(Properties properties){
        LabelHttpUtil.url = properties.getProperty("label.http.url");
    }

    public static Map<String,Object> post(String body){
        try{

            Map<String,Object> result = JsonUtil.toJavaBean(HttpClientUtil.postJson(url, body),Map.class);
            return result;
        }catch (Exception e){
            logger.error("ship server labelpost error: ", e);
        }
        return new HashMap<>();
    }

}
