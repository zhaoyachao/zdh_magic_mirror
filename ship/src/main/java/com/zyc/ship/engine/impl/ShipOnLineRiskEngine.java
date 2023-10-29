package com.zyc.ship.engine.impl;

import com.alibaba.fastjson.JSON;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.entity.*;
import com.zyc.ship.service.StrategyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

            //检测是否存在小流量区间,不存在则创建默认小流量区间 todo 默认根据uid hash

            //获取需要校验的策略信息
            List<StrategyGroupInstance> strategy_groups = getStrategyGroups(this.strategyService,scene, data_node);
            logger.info("uuid: {}, data_node: {}, hit strategy_groups: {}",uuid, data_node, JSON.toJSONString(strategy_groups));
            Map<String, Object> labels = new HashMap<>();
            Map<String, Object> filters = new HashMap<>();
            loadBaseData(null, labels, filters, shipCommonInputParam);

            logger.info("uuid: {}, label_values: {}", uuid, JSON.toJSONString(labels));


            //遍历策略信息,对每一个策略解析,拉取标签结果,后期确定是否采用disruptor
            Map<String, Map<String, ShipResult>> result = new ConcurrentHashMap<>();
            CountDownLatch groupCountDownLatch = new CountDownLatch(strategy_groups.size());
            Map<String, ShipEvent> shipEventMap = new ConcurrentHashMap<>();
            executeStrategyGroups(strategy_groups, labels, filters, shipCommonInputParam, data_node, groupCountDownLatch, shipEventMap, result);

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
