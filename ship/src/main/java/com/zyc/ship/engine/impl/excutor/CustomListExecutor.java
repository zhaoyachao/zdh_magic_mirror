package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

public class CustomListExecutor {

    public String execute(JSONObject run_jsmind_data, String uid){
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String name_list_str = run_jsmind_data.getOrDefault("name_list","").toString();
            tmp = String.valueOf(Sets.newHashSet(name_list_str.split(",")).contains(uid));
        }catch (Exception e){

        }
        return tmp;
    }
}
