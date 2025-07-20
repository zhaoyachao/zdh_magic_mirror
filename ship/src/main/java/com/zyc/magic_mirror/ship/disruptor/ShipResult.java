package com.zyc.magic_mirror.ship.disruptor;

import com.zyc.magic_mirror.ship.entity.StrategyEventResult;

import java.util.Map;

public interface ShipResult {

    public String getStartTime();

    public void setStartTime(String startTime);

    public String getEndTime();

    public void setEndTime(String endTime);

    public String getCostTime();

    public void setCostTime(String costTime) ;

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

    public Map<String, Object> getObjMap();

    public void setObjMap(Map<String, Object> objMap);

    public void addObj2Map(String key, Object value);

}
