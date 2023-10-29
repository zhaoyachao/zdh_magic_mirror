package com.zyc.ship.service;

import com.zyc.ship.entity.StrategyGroupInstance;

import java.util.List;

public interface StrategyService {

    public List<StrategyGroupInstance> selectBySceneAndDataNode(String scene, String data_node);

}
