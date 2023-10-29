package com.zyc.ship.disruptor;

import com.alibaba.fastjson.JSON;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.WorkHandler;
import com.zyc.common.entity.StrategyInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * worker disruptor , 处理策略任务
 */
public class ShipWorkerEventWorkHandler implements WorkHandler<ShipEvent> {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    private String handlerId;

    public ShipWorkerEventWorkHandler(){
    }

    public ShipWorkerEventWorkHandler(String handlerId){
        this.handlerId = handlerId;
    }

    @Override
    public void onEvent(ShipEvent shipEvent) throws Exception {
        try{
            if(shipEvent.getStopFlag().getFlag()){
                shipEvent.getRunPath().put(shipEvent.getStrategyInstanceId(), ShipConst.STATUS_ERROR);
            }
            logger.info("worker: "+ JSON.toJSONString(shipEvent));
            logger.info("worker check: "+ JSON.toJSONString(shipEvent.getStrategyInstance().getStrategy_context()));
            StrategyInstance strategyInstance = shipEvent.getStrategyInstance();
            if(shipEvent.getStatus().equalsIgnoreCase(ShipConst.STATUS_ERROR)){
                shipEvent.getRunPath().put(shipEvent.getStrategyInstanceId(), ShipConst.STATUS_ERROR);
            }
            if(shipEvent.getStatus().equalsIgnoreCase(ShipConst.STATUS_CREATE)){
                //执行节点任务
                ShipResult shipResult = shipEvent.getShipExecutor().execute(strategyInstance);
                shipEvent.getRunPath().put(shipEvent.getStrategyInstanceId(), shipResult.getStatus());
                String strategyInstanceId = shipEvent.getStrategyInstanceId();
                shipEvent.getShipResultMap().put(strategyInstanceId, shipResult);
            }
            Set<String> childrens = shipEvent.getDag().getChildren(shipEvent.getStrategyInstanceId());
            if(childrens != null){
                for(String children: childrens){
                    ShipEvent shipEvent1 = reBuildShipEvent(shipEvent, children);
                    EventTranslator<ShipEvent> eventEventTranslator = DisruptorManager.buildByShipEvent(shipEvent1);
                    shipEvent.getMasterDisruptor().publishEvent(eventEventTranslator);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            shipEvent.getCdl().countDown();
            if(shipEvent.getCdl().getCount() == 0){
                shipEvent.getGroupCdl().countDown();
            }
            shipEvent.clear();
        }
    }


    public ShipEvent reBuildShipEvent(ShipEvent shipEvent, String strategyId){
        ShipEvent shipEvent1 = new ShipEvent();
        shipEvent1.setStrategyInstanceId(strategyId);
        shipEvent1.setStrategyInstance(shipEvent.getStrategyInstanceMap().get(strategyId));
        shipEvent1.setStrategyInstanceMap(shipEvent.getStrategyInstanceMap());
        shipEvent1.setRunPath(shipEvent.getRunPath());
        shipEvent1.setDag(shipEvent.getDag());
        shipEvent1.setStopFlag(shipEvent.getStopFlag());
        shipEvent1.setMasterDisruptor(shipEvent.getMasterDisruptor());
        shipEvent1.setWorkerDisruptor(shipEvent.getWorkerDisruptor());
        shipEvent1.setShipResultMap(shipEvent.getShipResultMap());
        shipEvent1.setCdl(shipEvent.getCdl());
        shipEvent1.setGroupCdl(shipEvent.getGroupCdl());
        shipEvent1.setInputParam(shipEvent.getInputParam());
        shipEvent1.setShipExecutor(shipEvent.getShipExecutor());
        shipEvent1.setLabelValues(shipEvent.getLabelValues());
        shipEvent1.setFilterValues(shipEvent.getFilterValues());
        return shipEvent1;
    }

    private Operate check(String operate){
        if(operate.equalsIgnoreCase("and")){
            return new AndOperateImpl();
        }else if(operate.equalsIgnoreCase("or")){
            return new OrOperateImpl();
        }else if(operate.equalsIgnoreCase("not")){
            return new NotOperateImpl();
        }
        return new AndOperateImpl();
    }
}
