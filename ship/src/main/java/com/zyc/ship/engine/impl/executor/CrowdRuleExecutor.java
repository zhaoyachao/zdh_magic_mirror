package com.zyc.ship.engine.impl.executor;


import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrowdRuleExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(CrowdRuleExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{

        }catch (Exception e){
            logger.error("ship excutor label error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
