package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

import java.util.Map;

public class FilterExecutor {

    public String executor(JSONObject run_jsmind_data, ShipEvent shipEvent, String uid){
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            String[] filters = run_jsmind_data.getString("rule_id").split(",");
            if(!isHitFilter(filters, shipEvent.getFilterValues(), uid)){
                tmp = ShipResultStatusEnum.ERROR.code;
            }
        }catch (Exception e){

        }
        return tmp;
    }

    public boolean isHitFilter(String[] filters, Map<String,Object> filterValues, String uid){
        if(filters==null || filters.length == 0){
            return true;
        }
        for (String filter: filters){
            if(filterValues.containsKey(filter)){
                return false;
            }
        }
        return true;
    }
}
