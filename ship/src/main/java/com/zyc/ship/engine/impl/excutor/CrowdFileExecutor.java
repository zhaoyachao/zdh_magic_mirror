package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrowdFileExecutor {
    private static Logger logger= LoggerFactory.getLogger(CrowdFileExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, String product_code, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String crowd_file_id = run_jsmind_data.getOrDefault("rule_id","").toString();
            //key: {product_code}_{crowd_file_id}_{uid}
            String key = StringUtils.join(product_code,"_crowd_file_", crowd_file_id, "_", uid);
            Object value = JedisPoolUtil.redisClient().get(key);
            if(value != null){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }
        }catch (Exception e){
            logger.error("ship excutor crowdfile error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
