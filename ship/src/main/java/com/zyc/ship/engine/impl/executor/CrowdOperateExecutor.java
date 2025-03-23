package com.zyc.ship.engine.impl.executor;


import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CrowdOperateExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(CrowdOperateExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        //到执行器时的运算符,都是可执行的,master disruptor会提前判断,因此一定返回success
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{

        }catch (Exception e){

        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
