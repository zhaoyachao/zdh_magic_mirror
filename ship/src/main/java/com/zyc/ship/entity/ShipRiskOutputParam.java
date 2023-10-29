package com.zyc.ship.entity;

import com.zyc.ship.disruptor.ShipResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShipRiskOutputParam implements OutputParam{

    private Map<String,Map<String, ShipResult>> strategyGroupResults = new HashMap<>();

    public Map<String, Map<String, ShipResult>> getStrategyGroupResults() {
        return strategyGroupResults;
    }

    public void setStrategyGroupResults(Map<String, Map<String, ShipResult>> strategyGroupResults) {
        this.strategyGroupResults = strategyGroupResults;
    }
}
