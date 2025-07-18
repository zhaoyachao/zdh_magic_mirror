package com.zyc.magic_mirror.ship.engine.impl.executor;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomListExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(CustomListExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String name_list_str = run_jsmind_data.getOrDefault("name_list","").toString();
            if(Sets.newHashSet(name_list_str.split(",")).contains(uid)){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }
        }catch (Exception e){
            logger.error("ship excutor customlist error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
