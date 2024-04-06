package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.groovy.GroovyFactory;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

import java.util.HashMap;
import java.util.Map;

public class CodeBlockExecutor {

    public String execute(JSONObject run_jsmind_data, StrategyInstance strategyInstance){
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            String code_type=run_jsmind_data.getOrDefault("code_type", "").toString();
            String command=run_jsmind_data.getOrDefault("command", "").toString();
            //String hashKey = MD5.create().digestHex(command);

            if(code_type.equalsIgnoreCase("groovy")){
                Map<String,Object> params = new HashMap<>();
                params.put("strategy_instance_id", strategyInstance.getId());
                params.put("strategy_instance", strategyInstance);
                boolean result =(boolean) GroovyFactory.execExpress(command, params);
                tmp = String.valueOf(result);
            }
        }catch (Exception e){

        }
        return tmp;
    }
}
