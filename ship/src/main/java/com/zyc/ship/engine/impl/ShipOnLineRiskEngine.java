package com.zyc.ship.engine.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.entity.*;
import com.zyc.ship.service.StrategyService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ship 实时风控决策,需要返回决策信息
 */
public class ShipOnLineRiskEngine extends ShipCommonEngine{

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    private InputParam inputParam;
    private StrategyService strategyService;
    public ShipOnLineRiskEngine(InputParam inputParam, StrategyService strategyService){
        this.inputParam=inputParam;
        this.strategyService=strategyService;
    }

    @Override
    public OutputParam execute() {
        ShipRiskOutputParam shipRiskOutputParam=new ShipRiskOutputParam();
        try{
            //解析参数
            ShipCommonInputParam shipCommonInputParam = (ShipCommonInputParam) this.inputParam;
            logger.info("解析参数: {}", JSON.toJSONString(shipCommonInputParam));
            String product_code = shipCommonInputParam.getProduct_code();
            //获取scene,data_type相关的策略
            String scene = shipCommonInputParam.getScene();
            //data_node可以理解为节点分类
            String data_node = shipCommonInputParam.getData_node();

            String uuid = UUID.randomUUID().toString();

            int flow = shipCommonInputParam.getFlow();
            //检测是否存在小流量区间,不存在则创建默认小流量区间 todo 默认根据uid hash

            //获取需要校验的策略信息
            List<StrategyGroupInstance> strategy_groups = getStrategyGroups(this.strategyService,scene, data_node);

            //校验分流
            List<StrategyGroupInstance> hit_strategy_groups = getHitStrategyGroups(flow, strategy_groups);
            Set<StrategyGroupInstance> not_hit_strategy_groups = Sets.difference(Sets.newHashSet(strategy_groups), Sets.newHashSet(hit_strategy_groups));
            Set not_hit_strategy_group_ids = not_hit_strategy_groups.stream().map(strategyGroupInstance -> strategyGroupInstance.getId()).collect(Collectors.toSet());

            logger.info("uuid: {}, data_node: {}, flow not hit strategy_groups: {}",uuid, data_node, JSON.toJSONString(not_hit_strategy_group_ids));
            logger.info("uuid: {}, data_node: {}, hit strategy_groups: {}",uuid, data_node, JSON.toJSONString(hit_strategy_groups));
            Map<String, Object> labels = new HashMap<>();
            Map<String, Object> filters = new HashMap<>();
            loadBaseData(null, labels, filters, shipCommonInputParam);

            logger.info("uuid: {}, label_values: {}", uuid, JSON.toJSONString(labels));


            //遍历策略信息,对每一个策略解析,拉取标签结果,后期确定是否采用disruptor
            Map<String, Map<String, ShipResult>> result = new ConcurrentHashMap<>();
            CountDownLatch groupCountDownLatch = new CountDownLatch(hit_strategy_groups.size());
            Map<String, ShipEvent> shipEventMap = new ConcurrentHashMap<>();
            executeStrategyGroups(hit_strategy_groups, labels, filters, shipCommonInputParam, data_node, groupCountDownLatch, shipEventMap, result);

            //30秒超时,关闭线程
            if(!groupCountDownLatch.await(1000*1000, TimeUnit.MILLISECONDS)){
               for (String group_instance_id: shipEventMap.keySet()){
                   if(shipEventMap.get(group_instance_id).getCdl().getCount() != 0){
                       shipEventMap.get(group_instance_id).getStopFlag().setFlag(true);
                   }
               }
            }
            shipRiskOutputParam.setStrategyGroupResults(result);
            logger.info("uuid: {}, end", uuid);
            return shipRiskOutputParam;
        }catch (Exception e){
            e.printStackTrace();
        }

        return shipRiskOutputParam;
    }
}
