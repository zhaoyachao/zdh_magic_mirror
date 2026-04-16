package com.zyc.magic_mirror.ship.util;

import com.zyc.magic_mirror.common.http.HttpAction;
import com.zyc.magic_mirror.common.util.HttpUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FilterHttpUtil {

    private static Logger logger= LoggerFactory.getLogger(FilterHttpUtil.class);

    private static String url;

    private static String signKey;

    public static void init(Properties properties){
        FilterHttpUtil.url = properties.getProperty("filter.http.url");
        FilterHttpUtil.signKey = properties.getProperty("variable.service.key");
    }

    public static Map<String,Object> post(Map<String, Object> body){
        try{

            String sign = HttpAction.generatSign(body, signKey);
            body.put("sign", sign);
            String json = HttpUtil.builder().header("connection", "keep-alive").postJSON(url, JsonUtil.formatJsonString(body));
            Map<String,Object> result = JsonUtil.toJavaBean(json,Map.class);
            return result;
        }catch (Exception e){
            logger.error("ship server filterpost error: ", e);
        }
        return new HashMap<>();
    }
}
