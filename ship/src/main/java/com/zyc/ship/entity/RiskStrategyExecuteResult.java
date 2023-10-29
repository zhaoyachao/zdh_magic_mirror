package com.zyc.ship.entity;

public class RiskStrategyExecuteResult implements StrategyExecuteResult{

    private String strategy_id;

    private String strategy_group_id;

    private String strategy_instance_id;

    private String strategy_group_instance_id;

    private boolean isSuccess;

    private int status;

    private StrategyEventResult riskEventResult;


    public String getStrategy_id() {
        return strategy_id;
    }

    public void setStrategy_id(String strategy_id) {
        this.strategy_id = strategy_id;
    }

    public String getStrategy_group_id() {
        return strategy_group_id;
    }

    public void setStrategy_group_id(String strategy_group_id) {
        this.strategy_group_id = strategy_group_id;
    }

    public String getStrategy_instance_id() {
        return strategy_instance_id;
    }

    public void setStrategy_instance_id(String strategy_instance_id) {
        this.strategy_instance_id = strategy_instance_id;
    }

    public String getStrategy_group_instance_id() {
        return strategy_group_instance_id;
    }

    public void setStrategy_group_instance_id(String strategy_group_instance_id) {
        this.strategy_group_instance_id = strategy_group_instance_id;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public StrategyEventResult getRiskEventResult() {
        return riskEventResult;
    }

    public void setRiskEventResult(StrategyEventResult riskEventResult) {
        this.riskEventResult = riskEventResult;
    }

    public static RiskStrategyExecuteResult build(boolean isSuccess){
        RiskStrategyExecuteResult ser = new RiskStrategyExecuteResult();
        ser.setSuccess(isSuccess);
        return ser;
    }

    public static RiskStrategyExecuteResult build(boolean isSuccess, String eventCode, String eventResult){
        RiskStrategyExecuteResult ser = new RiskStrategyExecuteResult();
        ser.setSuccess(isSuccess);
        ser.setRiskEventResult(new RiskStrategyEventResult(eventCode, eventResult));
        return ser;
    }
}
