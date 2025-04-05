package com.zyc.magic_mirror.ship.log;

public class CommonLog {

    private String stage_code;

    private String status;

    private String reason;

    private String request_id;

    private String strategy_group_instance_id;

    private String strategy_instance_id;

    public String getStage_code() {
        return stage_code;
    }

    public void setStage_code(String stage_code) {
        this.stage_code = stage_code;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getRequest_id() {
        return request_id;
    }

    public void setRequest_id(String request_id) {
        this.request_id = request_id;
    }

    public String getStrategy_group_instance_id() {
        return strategy_group_instance_id;
    }

    public void setStrategy_group_instance_id(String strategy_group_instance_id) {
        this.strategy_group_instance_id = strategy_group_instance_id;
    }

    public String getStrategy_instance_id() {
        return strategy_instance_id;
    }

    public void setStrategy_instance_id(String strategy_instance_id) {
        this.strategy_instance_id = strategy_instance_id;
    }
}
