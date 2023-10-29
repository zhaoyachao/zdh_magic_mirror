package com.zyc.ship.engine.impl;


import com.alibaba.fastjson.JSON;
import com.zyc.ship.conf.ShipConf;
import com.zyc.ship.entity.OutputParam;
import com.zyc.ship.entity.ShipCommonInputParam;
import com.zyc.ship.service.impl.CacheStrategyServiceImpl;
import com.zyc.ship.util.FilterHttpUtil;
import com.zyc.ship.util.LabelHttpUtil;
import org.junit.Test;

import java.util.Properties;


public class ShipOnLineRiskEngineTest {


    @Test
    public void execute() {
        for (int i=0;i<1;i++){
            Properties properties=new Properties();
            properties.setProperty("label.http.url", "http://127.0.0.1:9003/api/v1/all");
            properties.setProperty("filter.http.url", "http://127.0.0.1:9003/api/v1/filter");
            properties.setProperty(ShipConf.ON_LINE_MANAGER_SYNC,"true");
            LabelHttpUtil.init(properties);
            FilterHttpUtil.init(properties);
            new ShipConf(properties);
            ShipCommonInputParam inputParam=new ShipCommonInputParam();
            inputParam.setUid("zyc");
            inputParam.setData_node("node_a");
            inputParam.setScene("online_risk");
            CacheStrategyServiceImpl cacheStrategyService = new CacheStrategyServiceImpl();
            cacheStrategyService.schedule();
            ShipOnLineRiskEngine shipOnLineRiskEngine=new ShipOnLineRiskEngine(inputParam, cacheStrategyService);
            OutputParam execute = shipOnLineRiskEngine.execute();
            System.out.println(JSON.toJSONString(execute));
        }
    }
}
