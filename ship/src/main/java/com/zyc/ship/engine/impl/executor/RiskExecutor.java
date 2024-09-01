package com.zyc.ship.engine.impl.executor;

import com.alibaba.fastjson.JSONObject;
import com.hubspot.jinjava.Jinjava;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import com.zyc.ship.entity.RiskStrategyEventResult;
import com.zyc.ship.entity.ShipCommonInputParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RiskExecutor extends BaseExecutor{

    private static Logger logger= LoggerFactory.getLogger(RiskExecutor.class);

    public ShipResult execute(ShipEvent shipEvent, JSONObject run_jsmind_data){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            String event_code = run_jsmind_data.getString("rule_id");
            String event_code_result = run_jsmind_data.getString("rule_param");

            //此处使用变量池结果
            Jinjava jinjava=new Jinjava();
            Map<String, Object> objectMap = new HashMap<>();
            String uid = ((ShipCommonInputParam)shipEvent.getInputParam()).getUid();
            objectMap.put("uid", uid);//获取当前结果集信息

            mergeMapByVarPool(shipEvent.getLogGroupId()+"", objectMap);
            event_code_result = jinjava.render(event_code_result, objectMap);//替换可变参数

            shipResult.setRiskStrategyEventResult(new RiskStrategyEventResult(event_code, event_code_result));
        }catch (Exception e){
            logger.error("ship excutor risk error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
