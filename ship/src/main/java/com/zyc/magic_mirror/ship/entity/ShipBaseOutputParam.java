package com.zyc.magic_mirror.ship.entity;

import com.zyc.magic_mirror.ship.disruptor.ShipResult;

import java.util.HashMap;
import java.util.Map;

public class ShipBaseOutputParam implements OutputParam{

    private String code;//具体错误码

    private String message;

    private String requestId;

    private String status;//success,error

    private Map<String,Map<String, ShipResult>> strategyGroupResults = new HashMap<>();

    public ShipBaseOutputParam() {

    }

    public ShipBaseOutputParam(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public ShipBaseOutputParam(String code, String message, String status) {
        this.code = code;
        this.message = message;
        this.status = status;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Map<String, Map<String, ShipResult>> getStrategyGroupResults() {
        return strategyGroupResults;
    }

    public void setStrategyGroupResults(Map<String, Map<String, ShipResult>> strategyGroupResults) {
        this.strategyGroupResults = strategyGroupResults;
    }
}
