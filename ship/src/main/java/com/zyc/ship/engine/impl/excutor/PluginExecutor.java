package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.excutor.plugin.HttpPlugin;
import com.zyc.ship.engine.impl.excutor.plugin.KafkaPlugin;
import com.zyc.ship.engine.impl.excutor.plugin.Plugin;

public class PluginExecutor {

    public String execute(JSONObject run_jsmind_data, String uid, StrategyInstance strategyInstance){
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点,当前不支持在线PLUGIN
            String rule_id = run_jsmind_data.getString("rule_id");

            Plugin plugin = getPlugin(rule_id, run_jsmind_data, strategyInstance);
            boolean result = plugin.execute();
            if(result){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }
            return tmp;
        }catch (Exception e){

        }
        return tmp;
    }


    private Plugin getPlugin(String rule_id, JSONObject run_jsmind_data, StrategyInstance strategyInstance) throws Exception {

        if(rule_id.equalsIgnoreCase("kafka")){
            return new KafkaPlugin(rule_id, run_jsmind_data, strategyInstance);
        }else if(rule_id.equalsIgnoreCase("rqueue")){

        }else if(rule_id.equalsIgnoreCase("http")){
            return new HttpPlugin(rule_id, run_jsmind_data, strategyInstance);
        }

        throw new Exception("not found plugin, rule_id: "+rule_id);
    }
}
