package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import com.zyc.ship.engine.impl.excutor.plugin.HttpPlugin;
import com.zyc.ship.engine.impl.excutor.plugin.KafkaPlugin;
import com.zyc.ship.engine.impl.excutor.plugin.Plugin;
import com.zyc.ship.engine.impl.excutor.plugin.ShipVariablePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginExecutor {
    private static Logger logger= LoggerFactory.getLogger(PluginExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, String uid, StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点,当前不支持在线PLUGIN
            String rule_id = run_jsmind_data.getString("rule_id");

            Plugin plugin = getPlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
            boolean result = plugin.execute();
            if(result){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }
        }catch (Exception e){
            logger.error("ship excutor plugin error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }


    private Plugin getPlugin(String rule_id, JSONObject run_jsmind_data, StrategyInstance strategyInstance, ShipEvent shipEvent) throws Exception {

        if(rule_id.equalsIgnoreCase("kafka")){
            return new KafkaPlugin(rule_id, run_jsmind_data, strategyInstance);
        }else if(rule_id.equalsIgnoreCase("rqueue")){

        }else if(rule_id.equalsIgnoreCase("http")){
            return new HttpPlugin(rule_id, run_jsmind_data, strategyInstance);
        }else if(rule_id.equalsIgnoreCase("ship_variable_expr")){
            return new ShipVariablePlugin(rule_id, run_jsmind_data, strategyInstance, shipEvent);
        }

        throw new Exception("not found plugin, rule_id: "+rule_id);
    }
}
