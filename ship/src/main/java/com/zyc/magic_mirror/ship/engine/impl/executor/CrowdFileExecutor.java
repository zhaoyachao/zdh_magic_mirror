package com.zyc.magic_mirror.ship.engine.impl.executor;

import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CrowdFileExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(CrowdFileExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String product_code, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String crowd_file_id = run_jsmind_data.getOrDefault("rule_id","").toString();
            //key: {product_code}_{crowd_file_id}_{uid}
            String key = StringUtils.join(product_code,"_crowd_file_", crowd_file_id, "_", uid);
            Object value = JedisPoolUtil.redisClient().get(key);
            if(value != null){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }else{
                shipResult.setMessage("未命中人群文件: "+key);
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
