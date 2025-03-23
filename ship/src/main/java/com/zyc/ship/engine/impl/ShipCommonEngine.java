package com.zyc.ship.engine.impl;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.DAG;
import com.zyc.common.util.JsonUtil;
import com.zyc.common.util.SnowflakeIdWorker;
import com.zyc.ship.common.Const;
import com.zyc.ship.conf.ShipConf;
import com.zyc.ship.disruptor.*;
import com.zyc.ship.engine.Engine;
import com.zyc.ship.entity.OutputParam;
import com.zyc.ship.entity.ShipCommonInputParam;
import com.zyc.ship.entity.StrategyGroupInstance;
import com.zyc.ship.log.CommonLog;
import com.zyc.ship.log.ShipOnlineRiskLog;
import com.zyc.ship.service.StrategyService;
import com.zyc.ship.util.FilterHttpUtil;
import com.zyc.ship.util.LabelHttpUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;

public class ShipCommonEngine implements Engine {

    private Logger logger= LoggerFactory.getLogger(this.getClass());


    /**
     * 从内存中获取所有满足的策略信息
     * @param scene
     * @param data_node
     * @return
     */
    public List<StrategyGroupInstance> getStrategyGroups(StrategyService strategyService, String scene, String data_node){
        List<StrategyGroupInstance> tmp = strategyService.selectBySceneAndDataNode(scene,data_node);
        if(tmp != null){
            return JsonUtil.toJavaListBean(JsonUtil.formatJsonString(tmp), StrategyGroupInstance.class);
        }
        return new ArrayList<>();
    }

    public List<StrategyGroupInstance> getHitStrategyGroups(int flow,List<StrategyGroupInstance> strategy_groups, HashSet<String> allocate_strategy_group_ids){
        List<StrategyGroupInstance> hit_strategy_groups = new ArrayList<>();
        //校验分流
        for (StrategyGroupInstance strategyGroupInstance: strategy_groups){
            CommonLog commonLog = new CommonLog();
            commonLog.setStrategy_group_instance_id(strategyGroupInstance.getId());
            commonLog.setStage_code("check_flows");
            commonLog.setStatus("1");
            commonLog.setReason("success");
            if(!StringUtils.isEmpty(strategyGroupInstance.getSmall_flow_rate())){
                String[] flows = strategyGroupInstance.getSmall_flow_rate().split(",",2);
                if(flows.length != 2){
                    //策略组分流{strategy_group_instance_id: ,is_hit: false, code: 1001}
                    commonLog.setReason("not hit strategy_group, flows is error");
                    commonLog.setStatus("2");//1成功,2失败
                    ShipOnlineRiskLog.info(commonLog);
                    continue;
                }
                if(flow >= Integer.valueOf(flows[0]) && flow <= Integer.valueOf(flows[1])){
                    //指定策略组id
                    if( allocate_strategy_group_ids.size() >0 && allocate_strategy_group_ids.contains(strategyGroupInstance.getGroup_id())){
                        hit_strategy_groups.add(strategyGroupInstance);
                    }else{
                        commonLog.setReason("not hit strategy_group, hit flows, but allocate_strategy_group_id not equlas current group_id");
                        hit_strategy_groups.add(strategyGroupInstance);
                    }
                    ShipOnlineRiskLog.info(commonLog);
                }else{
                    commonLog.setReason("not hit strategy_group, not hit flows");
                    commonLog.setStatus("2");//1成功,2失败
                    ShipOnlineRiskLog.info(commonLog);
                }
            }
        }
        return hit_strategy_groups;
    }

    /**
     * 加载标签和过滤集
     * @param executorService
     * @param labels
     * @param filters
     * @param shipCommonInputParam
     * @throws Exception
     */
    public void loadBaseData(ExecutorService executorService, Map<String, Object> labels,Map<String, Object> filters, ShipCommonInputParam shipCommonInputParam) throws Exception {
        String product_code = shipCommonInputParam.getProduct_code();
        String uid  = shipCommonInputParam.getUid();
        String id_type = shipCommonInputParam.getId_type();
        if(ShipConf.getOnLineManagerSync()){
            Future<Map<String, Object>> futureLabel = syncGetLabelByUser(executorService, null, product_code, uid, id_type);
            Future<Map<String, Object>> futureFilter = syncGetFilterByUser(executorService, null, product_code, uid, id_type);
            getResult(executorService, futureLabel, futureFilter,labels, filters);
        }else{
            Map<String, Object> labels_tmp = getLabelByUser(null, product_code,uid, id_type);
            labels.putAll(labels_tmp);
            filters = getFilterByUser(null, product_code, uid, id_type);
        }
        //解析参数,增加传递过来的标签
        if(!StringUtils.isEmpty(shipCommonInputParam.getParam())){
            Map<String, Object> jsonObject = JsonUtil.toJavaMap(shipCommonInputParam.getParam());
            //遍历tag_开头的标签,此处一定需要注意提前映射
            for (String key: jsonObject.keySet()){
                if(key.startsWith(Const.LABEL_PARAM_PRE)){
                    Map paramValues = (Map<String, Object>)jsonObject.get(key);
                    labels.put(key, JsonUtil.formatJsonString(paramValues));
                }
            }
        }
    }


    /**
     * 初始化dag图
     * @param strategy_group
     */
    public void initDag(StrategyGroupInstance strategy_group){
        List<Map<String,String>> dagMap = strategy_group.getDagMap();
        DAG dag = new DAG();
        for (Map<String,String> map: dagMap){
            dag.addEdge(map.get("from"), map.get("to"));
        }
        strategy_group.setDag(dag);
    }


    public void getResult(ExecutorService executorService,Future<Map<String, Object>> futureLabel,Future<Map<String, Object>> futureFilter, Map<String, Object> resultLabel, Map<String, Object> filters) throws Exception {
        try{
            resultLabel = futureLabel.get(12000, TimeUnit.MILLISECONDS);
            filters = futureFilter.get(12000, TimeUnit.MILLISECONDS);
        }catch (InterruptedException e){
            throw new Exception("get label or filter server, error", e);
        }catch (TimeoutException e){
            executorService.shutdown();
            throw new Exception("get label or filter server, time out", e);
        }
    }

    /**
     * 异步获取标签信息
     * @param executorService
     * @param strategy
     * @param uid
     * @param id_type
     * @return
     */
    public Future<Map<String,Object>> syncGetLabelByUser(ExecutorService executorService,StrategyInstance strategy, String product_code, String uid, String id_type){
        Future<Map<String, Object>> future = executorService.submit(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() throws Exception {
                try{
                    return getLabelByUser(null, product_code,uid, id_type);
                }catch (Exception e){
                   logger.error("ship common syncGetLabelByUser error: ", e);
                }finally {

                }
                return null;
            }
        });
        return future;
    }

    /**
     * 异步获取过滤集信息
     * @param executorService
     * @param strategy
     * @param uid
     * @param id_type
     * @return
     */
    public Future<Map<String,Object>> syncGetFilterByUser(ExecutorService executorService,StrategyInstance strategy, String product_code, String uid, String id_type){
        Future<Map<String, Object>> future = executorService.submit(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() throws Exception {
                try{
                    return getFilterByUser(null, product_code,uid, id_type);
                }catch (Exception e){
                    logger.error("ship common syncGetFilterByUser error: ", e);
                }finally {

                }
                return null;
            }
        });
        return future;
    }

    /**
     * 获取标签结果
     * @param strategy
     * @param uid
     * @param id_type
     * @return
     */
    public Map<String, Object> getLabelByUser(StrategyInstance strategy, String product_code, String uid, String id_type){
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> jsonObject = JsonUtil.createEmptyLinkMap();
        jsonObject.put("uid", uid);
        jsonObject.put("product_code", product_code);
        result = LabelHttpUtil.post(JsonUtil.formatJsonString(jsonObject));
        return result;
    }

    /**
     * 获取用户命中的过滤规则
     * @param strategy
     * @param uid
     * @param id_type
     * @return
     */
    public Map<String, Object> getFilterByUser(StrategyInstance strategy, String product_code, String uid, String id_type){
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> jsonObject = JsonUtil.createEmptyLinkMap();
        jsonObject.put("uid", uid);
        jsonObject.put("product_code", product_code);
        result = FilterHttpUtil.post(JsonUtil.formatJsonString(jsonObject));
        return result;
    }

    @Override
    public OutputParam execute() {
        return null;
    }


    /**
     * 执行命中策略组
     * @param strategy_groups
     * @param labels
     * @param filters
     * @param shipCommonInputParam
     * @param data_node
     * @param groupCountDownLatch
     * @param shipEventMap
     * @param result
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void executeStrategyGroups(List<StrategyGroupInstance> strategy_groups, Map<String, Object> labels, Map<String, Object> filters,
                                      ShipCommonInputParam shipCommonInputParam, String data_node, CountDownLatch groupCountDownLatch,
                                      Map<String, ShipEvent> shipEventMap, Map<String, Map<String, ShipResult>> result, String request_id) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        for (StrategyGroupInstance strategy_group: strategy_groups) {

            Map<String, ShipResult> shipResultMap = new HashMap<>();
            ShipEvent shipEvent = new ShipEvent();

            shipEvent.setRequestId(request_id);
            shipEvent.setLogGroupId(SnowflakeIdWorker.getInstance().nextId());
            StopFlag stopFlag = new StopFlag();
            shipEvent.setLabelValues(labels);
            shipEvent.setFilterValues(filters);

            Disruptor master = DisruptorManager.getDisruptor("ship_master", 1, new ShipMasterEventWorkHandler(), Integer.valueOf(ShipConf.getProperty(ShipConf.SHIP_DISRUPTOR_MASTER_RING_NUM, "1024")));
            Disruptor worker = DisruptorManager.getDisruptor("ship_worker", 1, new ShipWorkerEventWorkHandler(), Integer.valueOf(ShipConf.getProperty(ShipConf.SHIP_DISRUPTOR_WORKER_RING_NUM, "1024")));


            shipEvent.setInputParam(shipCommonInputParam);

            shipEvent.setMasterDisruptor(master);
            shipEvent.setWorkerDisruptor(worker);

            initDag(strategy_group);
            shipEvent.setDag(strategy_group.getDag());
            Map<String, String> runPath = new HashMap<>();
            shipEvent.setRunPath(runPath);
            shipEvent.setStrategyInstanceMap(strategy_group.getStrategyMap());
            shipEvent.setStopFlag(stopFlag);

            String root_strategy_id = "";
            for (String id : strategy_group.getRoot_strategys()) {
                StrategyInstance strategyInstance = strategy_group.getStrategyMap().get(id);
                if (strategyInstance != null && strategyInstance.getInstance_type().equalsIgnoreCase("data_node")) {
                    if (strategyInstance.getData_node().equalsIgnoreCase(data_node)) {
                        shipEvent.setStrategyInstance(strategyInstance);
                        shipEvent.setStrategyInstanceId(strategyInstance.getId());
                        root_strategy_id = strategyInstance.getId();
                        break;
                    }
                }
            }
            Set<String> childrens = shipEvent.getDag().getAllChildren(root_strategy_id);
            CountDownLatch cdl = new CountDownLatch(childrens.size());
            shipEvent.setCdl(cdl);
            shipEvent.setGroupCdl(groupCountDownLatch);
            shipEvent.setStatus(ShipConst.STATUS_CREATE);

            shipEvent.setStrategyGroupInstanceId(strategy_group.getId());
            shipEvent.setShipResultMap(shipResultMap);

            ShipExecutor shipExecutor = new RiskShipExecutorImpl(shipEvent);
            shipEvent.setShipExecutor(shipExecutor);
            shipEvent.setRunParam(new ConcurrentHashMap<>());

            shipEvent.setDagMap(strategy_group.getDagMap());
            EventTranslator<ShipEvent> eventEventTranslator = DisruptorManager.buildByShipEvent(shipEvent);

            master.publishEvent(eventEventTranslator);
            result.put(strategy_group.getId(), shipResultMap);
            shipEventMap.put(strategy_group.getId(), shipEvent);
        }
    }

}
