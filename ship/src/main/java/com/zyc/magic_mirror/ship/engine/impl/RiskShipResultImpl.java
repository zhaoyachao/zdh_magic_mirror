package com.zyc.magic_mirror.ship.engine.impl;

import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.entity.StrategyEventResult;

import java.util.HashMap;
import java.util.Map;

public class RiskShipResultImpl implements ShipResult {

    private String startTime;

    private String endTime;

    private String costTime;

    private int sequence=0;

    private String strategyInstanceId;

    private String status;

    private String message;

    private String strategyName;

    private StrategyEventResult riskStrategyEventResult;

    private Map<String, Object> objMap = new HashMap<>();

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
    public String getCostTime() {
        return costTime;
    }

    @Override
    public void setCostTime(String costTime) {
        this.costTime = costTime;
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
        return strategyInstanceId;
    }

    @Override
    public void setStrategyInstanceId(String straategyInstanceId) {
        this.strategyInstanceId = straategyInstanceId;
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

    @Override
    public Map<String, Object> getObjMap() {
        return objMap;
    }

    @Override
    public void setObjMap(Map<String, Object> objMap) {
        this.objMap = objMap;
    }

    @Override
    public void addObj2Map(String key, Object value) {
        this.objMap.put(key, value);
    }

}
