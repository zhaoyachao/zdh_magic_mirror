package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomListExecutor {
    private static Logger logger= LoggerFactory.getLogger(CustomListExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String name_list_str = run_jsmind_data.getOrDefault("name_list","").toString();
            tmp = String.valueOf(Sets.newHashSet(name_list_str.split(",")).contains(uid));
        }catch (Exception e){
            logger.error("ship excutor customlist error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
