package com.zyc.ship.disruptor;

import com.zyc.common.entity.StrategyInstance;

public interface ShipExecutor {

    public ShipResult execute(StrategyInstance strategyInstance);
}
