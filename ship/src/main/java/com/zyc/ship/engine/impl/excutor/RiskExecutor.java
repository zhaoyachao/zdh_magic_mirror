package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.entity.RiskStrategyEventResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiskExecutor {

    private static Logger logger= LoggerFactory.getLogger(RiskExecutor.class);

    public String execute(JSONObject run_jsmind_data, ShipResult shipResult){
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            String event_code = run_jsmind_data.getString("rule_id");
            String event_code_result = run_jsmind_data.getString("rule_param");
            shipResult.setRiskStrategyEventResult(new RiskStrategyEventResult(event_code, event_code_result));
        }catch (Exception e){
            logger.error("ship excutor risk error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
        }
        return tmp;
    }
}
