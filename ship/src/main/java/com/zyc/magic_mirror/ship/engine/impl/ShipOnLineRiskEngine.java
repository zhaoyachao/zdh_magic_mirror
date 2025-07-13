package com.zyc.magic_mirror.ship.engine.impl;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.SnowflakeIdWorker;
import com.zyc.magic_mirror.ship.common.Const;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.entity.*;
import com.zyc.magic_mirror.ship.exception.ErrorCode;
import com.zyc.magic_mirror.ship.service.StrategyService;
import com.zyc.rqueue.RQueueManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * ship 实时风控决策,需要返回决策信息
 */
public class ShipOnLineRiskEngine extends ShipCommonEngine{

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    public static ExecutorService executorService= new ThreadPoolExecutor(10, 1024, 500,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("ship_online_risk_").build());

    private InputParam inputParam;
    private StrategyService strategyService;
    public ShipOnLineRiskEngine(InputParam inputParam, StrategyService strategyService){
        this.inputParam=inputParam;
        this.strategyService=strategyService;
    }

    @Override
    public OutputParam execute() {
        ShipBaseOutputParam shipBaseOutputParam=new ShipBaseOutputParam();
        shipBaseOutputParam.setStatus(Const.STATUS_SUCCESS);
        shipBaseOutputParam.setCode(ErrorCode.SUCCESS_CODE);

        long start_time = System.currentTimeMillis();
        long request_id= SnowflakeIdWorker.getInstance().nextId();
        String request_id_str = DateUtil.format(new Date(), "yyyyMMddHH") + request_id;

        shipBaseOutputParam.setRequestId(request_id_str);

        try{
            //解析参数
            ShipCommonInputParam shipCommonInputParam = (ShipCommonInputParam) this.inputParam;
            logger.info("解析参数: {}", JsonUtil.formatJsonString(shipCommonInputParam));
            String product_code = shipCommonInputParam.getProduct_code();
            //获取scene,data_type相关的策略
            String scene = shipCommonInputParam.getScene();
            //data_node可以理解为节点分类
            String data_node = shipCommonInputParam.getData_node();

            int flow = shipCommonInputParam.getFlow();
            //检测是否存在小流量区间,不存在则创建默认小流量区间 todo 默认根据uid hash

            //校验是否跳过,防止出现问题导致重大损失使用
            Object risk_is_stop= JedisPoolUtil.redisClient().get(Const.ONLINE_RISK_IS_STOP_KEY);
            if(risk_is_stop != null && risk_is_stop.toString().equalsIgnoreCase("true")){
                return shipBaseOutputParam;
            }

            //获取需要校验的策略信息
            List<StrategyGroupInstance> strategy_groups = getStrategyGroups(this.strategyService,scene, data_node);

            String allocate_strategy_group_ids = shipCommonInputParam.getAllocate_strategy_group_ids();
            HashSet allocate_strategy_group_id_hashset = Sets.newHashSet();
            if(!StringUtils.isEmpty(allocate_strategy_group_ids)){
                allocate_strategy_group_id_hashset =  Sets.newHashSet(allocate_strategy_group_ids.split(","));
            }
            //校验分流
            List<StrategyGroupInstance> hit_strategy_groups = getHitStrategyGroups(flow, strategy_groups, allocate_strategy_group_id_hashset);
            Set<StrategyGroupInstance> not_hit_strategy_groups = Sets.difference(Sets.newHashSet(strategy_groups), Sets.newHashSet(hit_strategy_groups));
            Set not_hit_strategy_group_ids = not_hit_strategy_groups.stream().map(strategyGroupInstance -> strategyGroupInstance.getId()).collect(Collectors.toSet());

            logger.info("request_id: {}, data_node: {}, flow not hit strategy_groups: {}",request_id_str, data_node, JsonUtil.formatJsonString(not_hit_strategy_group_ids));
            logger.info("request_id: {}, data_node: {}, hit strategy_groups: {}",request_id_str, data_node, JsonUtil.formatJsonString(hit_strategy_groups));

            if(hit_strategy_groups == null || hit_strategy_groups.size() <= 0){
                shipBaseOutputParam.setCode(ErrorCode.NOT_HIG_STRATEGY_ERROR_CODE);
                return shipBaseOutputParam;
            }

            Map<String, Object> labels = new HashMap<>();
            Map<String, Object> filters = new HashMap<>();
            loadBaseData(executorService, labels, filters, shipCommonInputParam);

            logger.info("request_id: {}, label_values: {}", request_id_str, JsonUtil.formatJsonString(labels));


            //遍历策略信息,对每一个策略解析,拉取标签结果,后期确定是否采用disruptor
            Map<String, Map<String, ShipResult>> result = new ConcurrentHashMap<>();
            CountDownLatch groupCountDownLatch = new CountDownLatch(hit_strategy_groups.size());
            Map<String, ShipEvent> shipEventMap = new ConcurrentHashMap<>();
            executeStrategyGroups(hit_strategy_groups, labels, filters, shipCommonInputParam, data_node, groupCountDownLatch, shipEventMap, result, request_id_str);

            //10秒超时,关闭线程
            if(!groupCountDownLatch.await(1000*10, TimeUnit.MILLISECONDS)){
               for (String group_instance_id: shipEventMap.keySet()){
                   if(shipEventMap.get(group_instance_id).getCdl().getCount() != 0){
                       shipEventMap.get(group_instance_id).getStopFlag().setFlag(true);
                   }
               }
            }
            RQueueManager.getRQueueClient(Const.SHIP_ONLINE_RISK_LOG_QUEUE).add(JsonUtil.formatJsonString(shipEventMap.values()));
            shipBaseOutputParam.setStrategyGroupResults(result);

            return shipBaseOutputParam;
        }catch (Exception e){
            logger.error("ship online risk execute error: ", e);
            shipBaseOutputParam.setCode(ErrorCode.EXECUTE_ERROR_CODE);
            shipBaseOutputParam.setMessage(e.getMessage());
            shipBaseOutputParam.setStatus(Const.STATUS_ERROR);
        }finally {
            long end_time = System.currentTimeMillis();
            logger.info("request_id: {}, cost_time: {}ms, end", request_id_str, end_time-start_time);
        }

        return shipBaseOutputParam;
    }
}
