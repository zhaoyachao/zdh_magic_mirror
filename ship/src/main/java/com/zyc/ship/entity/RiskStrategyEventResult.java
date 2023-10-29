package com.zyc.ship.entity;

public class RiskStrategyEventResult implements StrategyEventResult{

    //事件
    private String eventCode;

    private String eventResult;

    public RiskStrategyEventResult(){}

    public RiskStrategyEventResult(String eventCode, String eventResult){
        this.eventCode = eventCode;
        this.eventResult = eventResult;
    }

    public String getEventCode() {
        return eventCode;
    }

    public void setEventCode(String eventCode) {
        this.eventCode = eventCode;
    }

    public String getEventResult() {
        return eventResult;
    }

    public void setEventResult(String eventResult) {
        this.eventResult = eventResult;
    }
}
