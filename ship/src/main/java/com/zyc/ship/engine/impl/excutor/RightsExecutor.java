package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

public class RightsExecutor {

    public String execute(JSONObject run_jsmind_data, String uid){
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点,当前不支持在线权益
            return tmp;
        }catch (Exception e){

        }
        return tmp;
    }
}
