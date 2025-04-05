package com.zyc.magic_mirror.ship.engine.impl.executor;

import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RightsExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(RightsExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String uid){
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
