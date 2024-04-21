package com.zyc.ship.engine.impl;

import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.entity.StrategyEventResult;

public class RiskShipResultImpl implements ShipResult {


    private String strategy_instance_id;

    private String status;

    private String strategyName;

    private StrategyEventResult riskStrategyEventResult;

    @Override
    public String getStrategyInstanceId() {
        return strategy_instance_id;
    }

    @Override
    public void setStrategyInstanceId(String straategyInstanceId) {
        this.strategy_instance_id = straategyInstanceId;
    }

    @Override
    public String getStatus() {
        return this.status;
    }

    @Override
    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String getStrategyName() {
        return strategyName;
    }

    @Override
    public void setStrategyName(String strategyName) {
        this.strategyName = strategyName;
    }

    @Override
    public void setRiskStrategyEventResult(StrategyEventResult riskStrategyEventResult) {
        this.riskStrategyEventResult = riskStrategyEventResult;
    }

    @Override
    public StrategyEventResult getRiskStrategyEventResult() {
        return riskStrategyEventResult;
    }


}
