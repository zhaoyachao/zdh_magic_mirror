package com.zyc.magic_mirror.ship.disruptor;

import com.zyc.magic_mirror.common.util.DAG;

import java.util.Map;

public interface Operate {

    /**
     * 返回值, error: 当前节点置为失败, wait: 当前节点等待, create: 当前节点可执行
     * @param strategyId
     * @param dag
     * @param runPath
     * @return  error: 当前节点置为失败, wait: 当前节点等待, create: 当前节点可执行
     */
    public String execute(String strategyId, DAG dag, Map<String, String> runPath);
}
