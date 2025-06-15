package com.zyc.magic_mirror.common.entity;

import java.sql.Timestamp;

public class StrategyLogInfo {

    private String strategy_instance_id;

    private String strategy_group_instance_id;

    private String strategy_group_id;

    private String strategy_id;

    /**
     * 带有动态日期的地址
     */
    private String base_path;

    /**
     * 本地文件写入完整路径
     */
    private String file_path;

    private String file_rocksdb_path;

    private Timestamp cur_time;

    private String instance_type;

    private String status;

    private String total_num;

    private String success_num;

    private String error_num;

    private StrategyGroupInstance strategyGroupInstance;

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

    public String getStrategy_group_id() {
        return strategy_group_id;
    }

    public void setStrategy_group_id(String strategy_group_id) {
        this.strategy_group_id = strategy_group_id;
    }

    public String getStrategy_id() {
        return strategy_id;
    }

    public void setStrategy_id(String strategy_id) {
        this.strategy_id = strategy_id;
    }

    public String getBase_path() {
        return base_path;
    }

    public void setBase_path(String base_path) {
        this.base_path = base_path;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getFile_rocksdb_path() {
        return file_rocksdb_path;
    }

    public void setFile_rocksdb_path(String file_rocksdb_path) {
        this.file_rocksdb_path = file_rocksdb_path;
    }

    public Timestamp getCur_time() {
        return cur_time;
    }

    public void setCur_time(Timestamp cur_time) {
        this.cur_time = cur_time;
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

    public String getTotal_num() {
        return total_num;
    }

    public void setTotal_num(String total_num) {
        this.total_num = total_num;
    }

    public String getSuccess_num() {
        return success_num;
    }

    public void setSuccess_num(String success_num) {
        this.success_num = success_num;
    }

    public String getError_num() {
        return error_num;
    }

    public void setError_num(String error_num) {
        this.error_num = error_num;
    }

    public StrategyGroupInstance getStrategyGroupInstance() {
        return strategyGroupInstance;
    }

    public void setStrategyGroupInstance(StrategyGroupInstance strategyGroupInstance) {
        this.strategyGroupInstance = strategyGroupInstance;
    }
}
