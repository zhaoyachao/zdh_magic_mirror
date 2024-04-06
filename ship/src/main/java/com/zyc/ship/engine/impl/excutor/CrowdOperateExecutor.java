package com.zyc.ship.engine.impl.excutor;


import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

public class CrowdOperateExecutor {

    public String execute(JSONObject run_jsmind_data, String uid){
        //到执行器时的运算符,都是可执行的,master disruptor会提前判断,因此一定返回success
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            return tmp;
        }catch (Exception e){

        }
        return tmp;
    }
}
