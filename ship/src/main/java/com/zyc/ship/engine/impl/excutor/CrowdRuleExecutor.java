package com.zyc.ship.engine.impl.excutor;


import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

public class CrowdRuleExecutor {

    public String execute(JSONObject run_jsmind_data, String uid){
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            return tmp;
        }catch (Exception e){

        }
        return tmp;
    }
}
