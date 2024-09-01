package com.zyc.ship.engine.impl.executor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FilterExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(FilterExecutor.class);

    public ShipResult executor(JSONObject run_jsmind_data, ShipEvent shipEvent, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            String[] filters = run_jsmind_data.getString("rule_id").split(",");
            if(!isHitFilter(filters, shipEvent.getFilterValues(), uid)){
                tmp = ShipResultStatusEnum.ERROR.code;
                shipResult.setMessage("命中过滤集");
            }
        }catch (Exception e){
            logger.error("ship excutor filter error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
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
