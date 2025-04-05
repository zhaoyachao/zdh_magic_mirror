package com.zyc.magic_mirror.ship.entity;

/**
 * 策略任务实例执行信息
 */
public class StrategyRunPath {

    private String id;

    private String strategy_context;


    /**
     * label,filter,shunt,touch,plugin,data_node,id_mapping
     */
    private String instance_type;

    /**
     * success,faild,created
     */
    private String status;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStrategy_context() {
        return strategy_context;
    }

    public void setStrategy_context(String strategy_context) {
        this.strategy_context = strategy_context;
    }

    public String getInstance_type() {
        return instance_type;
    }

    public void setInstance_type(String instance_type) {
        this.instance_type = instance_type;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
