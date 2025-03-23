package com.zyc.ship.engine.impl.executor;

import com.zyc.common.entity.StrategyInstance;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import com.zyc.ship.engine.impl.executor.plugin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PluginExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(PluginExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String uid, StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点,当前不支持在线PLUGIN
            String rule_id = run_jsmind_data.get("rule_id").toString();

            Plugin plugin = getPlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
            boolean result = plugin.execute();
            if(result){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }else{
                shipResult.setMessage("查询执行失败: "+plugin.getName());
            }
        }catch (Exception e){
            logger.error("ship excutor plugin error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }


    private Plugin getPlugin(String rule_id, Map<String, Object> run_jsmind_data, StrategyInstance strategyInstance, ShipEvent shipEvent) throws Exception {

        if(rule_id.equalsIgnoreCase("kafka")){
            return new KafkaPlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
        }else if(rule_id.equalsIgnoreCase("rqueue")){
            return new RqueuePlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
        }else if(rule_id.equalsIgnoreCase("http")){
            return new HttpPlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
        }else if(rule_id.equalsIgnoreCase("ship_variable_expr")){
            return new ShipVariablePlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
        }

        throw new Exception("not found plugin, rule_id: "+rule_id);
    }
}
