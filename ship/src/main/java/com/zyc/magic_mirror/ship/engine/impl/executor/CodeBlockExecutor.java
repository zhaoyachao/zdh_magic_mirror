package com.zyc.magic_mirror.ship.engine.impl.executor;

import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.groovy.GroovyFactory;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CodeBlockExecutor extends BaseExecutor{

    private static Logger logger= LoggerFactory.getLogger(CodeBlockExecutor.class);

    public ShipResult execute(ShipEvent shipEvent, Map<String, Object> run_jsmind_data, StrategyInstance strategyInstance){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String code_type=run_jsmind_data.getOrDefault("code_type", "").toString();
            String command=run_jsmind_data.getOrDefault("command", "").toString();
            //String hashKey = MD5.create().digestHex(command);

            if(code_type.equalsIgnoreCase("groovy")){
                Map<String,Object> params = new HashMap<>();
                params.putAll(shipEvent.getRunParam());
                params.put("strategy_instance_id", strategyInstance.getId());
                params.put("strategy_instance", strategyInstance);

                boolean result =(boolean) GroovyFactory.execExpress(command, params);
                tmp = String.valueOf(result);
            }
        }catch (Exception e){
            logger.error("ship excutor codeblock error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
