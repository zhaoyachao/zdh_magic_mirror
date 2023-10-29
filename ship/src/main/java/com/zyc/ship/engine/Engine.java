package com.zyc.ship.engine;

import com.zyc.ship.entity.OutputParam;
import com.zyc.ship.entity.StrategyExecuteResult;
import com.zyc.ship.entity.StrategyGroupInstance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public interface Engine {

    public OutputParam execute();
}
