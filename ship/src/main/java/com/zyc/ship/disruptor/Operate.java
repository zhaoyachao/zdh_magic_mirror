package com.zyc.ship.disruptor;

import com.zyc.common.util.DAG;

import java.util.Map;

public interface Operate {

    public String execute(String strategyId, DAG dag, Map<String, String> runPath);
}
