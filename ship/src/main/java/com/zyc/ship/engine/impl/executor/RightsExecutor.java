package com.zyc.ship.engine.impl.executor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RightsExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(RightsExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点,当前不支持在线权益

        }catch (Exception e){
            logger.error("ship excutor rights error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
