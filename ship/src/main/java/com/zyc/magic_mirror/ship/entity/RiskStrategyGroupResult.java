package com.zyc.magic_mirror.ship.entity;

import java.util.List;

/**
 * 实时风控类策略决策结果-数据结构(返回值)
 * 一个策略组下 有多个决策结果（）
 */
public class RiskStrategyGroupResult implements StrategyGroupResult {

    private List<StrategyExecuteResult> strategyExecuteResultList;

    public List<StrategyExecuteResult> getStrategyExecuteResultList() {
        return strategyExecuteResultList;
    }

    public void setStrategyExecuteResultList(List<StrategyExecuteResult> strategyExecuteResultList) {
        this.strategyExecuteResultList = strategyExecuteResultList;
    }
}
