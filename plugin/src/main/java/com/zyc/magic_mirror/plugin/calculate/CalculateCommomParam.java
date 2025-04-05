package com.zyc.magic_mirror.plugin.calculate;

import com.zyc.magic_mirror.common.entity.StrategyInstance;

import java.util.List;

public class CalculateCommomParam {

    private String group_id;

    private String group_instance_id;

    private String pre_tasks;

    private List<StrategyInstance> strategyInstanceList;

    public CalculateCommomParam(Builder builder){
        this.pre_tasks=builder.pre_tasks;
        this.strategyInstanceList=builder.strategyInstanceList;
        this.group_id=builder.group_id;
        this.group_instance_id=builder.group_instance_id;

    }

    public String getPre_tasks() {
        return pre_tasks;
    }

    public void setPre_tasks(String pre_tasks) {
        this.pre_tasks = pre_tasks;
    }

    public List<StrategyInstance> getStrategyInstanceList() {
        return strategyInstanceList;
    }

    public void setStrategyInstanceList(List<StrategyInstance> strategyInstanceList) {
        this.strategyInstanceList = strategyInstanceList;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getGroup_instance_id() {
        return group_instance_id;
    }

    public void setGroup_instance_id(String group_instance_id) {
        this.group_instance_id = group_instance_id;
    }

    public static class Builder{

        private String group_id;

        private String group_instance_id;

        private String pre_tasks="";

        private List<StrategyInstance> strategyInstanceList;

        public Builder(){

        }

        public Builder group_id(String group_id){
            this.group_id = group_id;
            return this;
        }

        public Builder group_instance_id(String group_instance_id){
            this.group_instance_id = group_instance_id;
            return this;
        }

        public Builder pre_tasks(String pre_tasks) {
            this.pre_tasks = pre_tasks;
            return this;
        }


        public Builder strategyInstanceList(List<StrategyInstance> strategyInstanceList) {
            this.strategyInstanceList = strategyInstanceList;
            return this;
        }

        public CalculateCommomParam build(){
            return new CalculateCommomParam(this);
        }
    }
}
