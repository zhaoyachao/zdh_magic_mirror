package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IdMappingExecutor {
    private static Logger logger= LoggerFactory.getLogger(IdMappingExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, Map<String,Object> labelVaues, ShipEvent shipEvent, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            String mapping_code = run_jsmind_data.getString("rule_id");
            String tag_key = "tag_"+mapping_code;
            Object value = labelVaues.get(tag_key);
            if(value == null){
                tmp = ShipResultStatusEnum.ERROR.code;
                shipResult.setMessage("无法找到id对应映射");
            }
            shipEvent.getRunParam().put(mapping_code, value);

        }catch (Exception e){
            logger.error("ship excutor idmapping error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
