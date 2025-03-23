package com.zyc.ship.engine.impl.executor.plugin;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.groovy.GroovyFactory;
import com.zyc.ship.disruptor.ShipEvent;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ship模块中间变量插件
 */
public class ShipVariablePlugin implements Plugin{
    private String rule_id;
    private Map<String, Object> run_jsmind_data;
    private StrategyInstance strategyInstance;
    private ShipEvent shipEvent;

    public ShipVariablePlugin(String rule_id, Map<String, Object> run_jsmind_data, StrategyInstance strategyInstance, ShipEvent shipEvent){
        this.rule_id = rule_id;
        this.run_jsmind_data = run_jsmind_data;
        this.strategyInstance = strategyInstance;
        this.shipEvent = shipEvent;
    }

    @Override
    public String getName() {
        return "shipvariable";
    }

    @Override
    public boolean execute() throws Exception {

        try {
            Gson gson = new Gson();
            List<Map> rule_params = gson.fromJson((run_jsmind_data).get("rule_param").toString(), new TypeToken<List<Map>>() {
            }.getType());

            Properties props = new Properties();

            for (Map<String, Object> param : rule_params) {
                String key = param.get("param_code").toString();
                String value = param.getOrDefault("param_value", "").toString();
                if (!StringUtils.isEmpty(value)) {
                    props.put(key, value);
                }
            }

            String expr = props.getProperty("expr", "");
            String expr_engine = props.getProperty("expr_engine", "java");

            Map<String,Object> params = new HashMap<>();
            params.put("runParam", shipEvent.getRunParam());

            boolean result =(boolean) GroovyFactory.execExpress(expr, params);
            return result;
        }catch (Exception e){
            throw e;
        }
    }
}
