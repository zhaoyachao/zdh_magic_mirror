package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipEvent;

import java.util.Map;

public class IdMappingExecutor {

    public String execute(JSONObject run_jsmind_data, Map<String,Object> labelVaues, ShipEvent shipEvent, String uid){
        String tmp = "success";
        try{
            String mapping_code = run_jsmind_data.getString("id_mapping_code");
            String tag_key = "tag_"+mapping_code;
            Object value = labelVaues.get(tag_key);
            if(value == null){
                tmp = "error";
            }
            shipEvent.getRunParam().put(mapping_code, value);

        }catch (Exception e){
            tmp = "error";
        }
        return tmp;
    }
}
