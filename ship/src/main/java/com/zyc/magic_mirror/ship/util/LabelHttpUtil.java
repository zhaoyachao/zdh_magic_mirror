package com.zyc.magic_mirror.ship.util;

import com.zyc.magic_mirror.common.http.HttpAction;
import com.zyc.magic_mirror.common.util.HttpClientUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LabelHttpUtil {

    private static Logger logger= LoggerFactory.getLogger(LabelHttpUtil.class);

    private static String url;

    private static String signKey;

    public static void init(Properties properties){
        LabelHttpUtil.url = properties.getProperty("variable.http.url");
        LabelHttpUtil.signKey = properties.getProperty("variable.service.key");
    }

    public static Map<String,Object> post(Map<String, Object> body){
        try{
            String sign = HttpAction.generatSign(body, signKey);
            body.put("sign", sign);

            Map<String,Object> result = JsonUtil.toJavaBean(HttpClientUtil.postJson(url, JsonUtil.formatJsonString(body)),Map.class);
            return result;
        }catch (Exception e){
            logger.error("ship server labelpost error: ", e);
        }
        return new HashMap<>();
    }
}
