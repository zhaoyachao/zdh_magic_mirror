package com.zyc.magic_mirror.ship.engine.impl;

import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.entity.StrategyEventResult;

public class RiskShipResultImpl implements ShipResult {

    private String startTime;

    private String endTime;

    private int sequence=0;

    private String strategy_instance_id;

    private String status;

    private String message;

    private String strategyName;

    private StrategyEventResult riskStrategyEventResult;

    @Override
    public String getStartTime() {
        return startTime;
    }

    @Override
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    @Override
    public String getEndTime() {
        return endTime;
    }

    @Override
    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

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

    @Override
    public String getMessage() {
        return this.message;
    }

    @Override
    public void setMessage(String message) {
        this.message = message;
    }


}
