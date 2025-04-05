package com.zyc.magic_mirror.ship.disruptor;

import com.zyc.magic_mirror.common.entity.StrategyInstance;

public interface ShipExecutor {

    public ShipResult execute(StrategyInstance strategyInstance);
}
