package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.redis.JedisPoolUtil;
import org.apache.commons.lang3.StringUtils;

public class CrowdFileExecutor {

    public String execute(JSONObject run_jsmind_data, String product_code, String uid){
        String tmp = "error";
        try{
            String crowd_file_id = run_jsmind_data.getOrDefault("crowd_file","").toString();
            //key: {product_code}_{crowd_file_id}_{uid}
            String key = StringUtils.join(product_code,"_crowd_file_", crowd_file_id, "_", uid);
            Object value = JedisPoolUtil.redisClient().get(key);
            if(value != null){
                tmp = "success";
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return tmp;
    }
}
