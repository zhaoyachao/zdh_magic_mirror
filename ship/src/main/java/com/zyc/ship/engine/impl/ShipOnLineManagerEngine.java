package com.zyc.ship.engine.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.DAG;
import com.zyc.ship.antlr4.SRLexer;
import com.zyc.ship.antlr4.SRParser;
import com.zyc.ship.antlr4.ShipSRListener;
import com.zyc.ship.disruptor.*;
import com.zyc.ship.entity.*;
import com.zyc.ship.service.StrategyService;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * ship 离线管理计算引擎
 * 只做实时接入流量,离线经营
 */
public class ShipOnLineManagerEngine extends ShipCommonEngine {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    public static ExecutorService executorService= new ThreadPoolExecutor(10, 10, 500,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().build(), new ThreadPoolExecutor.AbortPolicy());

    private InputParam inputParam;
    private StrategyService strategyService;
    public ShipOnLineManagerEngine(InputParam inputParam, StrategyService strategyService){
        this.inputParam=inputParam;
        this.strategyService=strategyService;
    }

    /**
     * 引擎入口
     * @return
     */
    @Override
    public OutputParam execute() {
        ShipManagerOutputParam shipManagerOutputParam = new ShipManagerOutputParam();
        shipManagerOutputParam.setStatus("error");
        try{
            ShipOnLineManagerEngine shipOnLineManagerEngine = this;
            String uuid = UUID.randomUUID().toString();
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try{
                        //解析参数
                        ShipCommonInputParam shipCommonInputParam = (ShipCommonInputParam) inputParam;
                        logger.info("解析参数: {}", JSON.toJSONString(shipCommonInputParam));
                        String product_code = shipCommonInputParam.getProduct_code();
                        //获取scene,data_type相关的策略
                        String scene = shipCommonInputParam.getScene();
                        //data_node可以理解为节点分类,根据data_node 找到策略组中开始的根节点
                        String data_node = shipCommonInputParam.getData_node();

                        int flow = shipCommonInputParam.getFlow();
                        //获取需要校验的策略信息
                        List<StrategyGroupInstance> strategy_groups = getStrategyGroups(strategyService,scene, data_node);

                        //校验分流
                        List<StrategyGroupInstance> hit_strategy_groups = getHitStrategyGroups(flow, strategy_groups);

                        logger.info("uuid: {}, data_node: {}, hit strategy_groups: {}",uuid, data_node, JSON.toJSONString(hit_strategy_groups));
                        Map<String, Object> labels = new HashMap<>();
                        Map<String, Object> filters = new HashMap<>();
                        shipOnLineManagerEngine.loadBaseData(null, labels, filters, shipCommonInputParam);

                        logger.info("uuid: {}, label_values: {}", uuid, JSON.toJSONString(labels));

                        //遍历策略信息,对每一个策略解析,拉取标签结果,后期确定是否采用disruptor
                        Map<String, Map<String, ShipResult>> result = new ConcurrentHashMap<>();
                        CountDownLatch groupCountDownLatch = new CountDownLatch(hit_strategy_groups.size());
                        Map<String, ShipEvent> shipEventMap = new ConcurrentHashMap<>();

                        shipOnLineManagerEngine.executeStrategyGroups(hit_strategy_groups, labels, filters, shipCommonInputParam, data_node, groupCountDownLatch, shipEventMap, result);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            });

            shipManagerOutputParam.setStatus("success");
            //shipRiskOutputParam.setStrategyGroupResults(futures);
            logger.info("uuid: {}, end", uuid);
        }catch (Exception e){
            e.printStackTrace();
        }

        return shipManagerOutputParam;
    }
}
