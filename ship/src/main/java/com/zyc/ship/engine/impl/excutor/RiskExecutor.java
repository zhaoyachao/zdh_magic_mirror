package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.entity.RiskStrategyEventResult;

public class RiskExecutor {

    public String execute(JSONObject run_jsmind_data, ShipResult shipResult){
        String tmp = "success";
        try{
            String event_code = run_jsmind_data.getString("rule_id");
            String event_code_result = run_jsmind_data.getString("rule_param");
            shipResult.setRiskStrategyEventResult(new RiskStrategyEventResult(event_code, event_code_result));
        }catch (Exception e){
            e.printStackTrace();
        }
        return tmp;
    }
}
