package com.zyc.ship.disruptor;

import com.zyc.ship.entity.RiskStrategyEventResult;
import com.zyc.ship.entity.StrategyEventResult;

public interface ShipResult {

    /**
     * create, wait, success, error
     * 只会返回success, error
     * @return
     */
    public String getStatus();

    public void setStatus(String status);

    public String getStrategyName();

    public void setStrategyName(String strategyName);

    public void setRiskStrategyEventResult(StrategyEventResult riskStrategyEventResult);

    public StrategyEventResult getRiskStrategyEventResult();

}
