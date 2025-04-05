package com.zyc.magic_mirror.ship.service;

import com.zyc.magic_mirror.ship.entity.StrategyGroupInstance;

import java.util.List;

public interface StrategyService {

    public List<StrategyGroupInstance> selectBySceneAndDataNode(String scene, String data_node);

}
