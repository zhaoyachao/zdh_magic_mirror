package com.zyc.ship.disruptor;

import com.zyc.ship.entity.StrategyEventResult;

public interface ShipResult {

    public String getStartTime();

    public void setStartTime(String startTime);

    public String getEndTime();

    public void setEndTime(String endTime);

    public int getSequence();

    public void setSequence(int sequence);

    public String getStrategyInstanceId();

    public void setStrategyInstanceId(String straategyInstanceId);

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

    public String getMessage();

    public void setMessage(String message);

}
