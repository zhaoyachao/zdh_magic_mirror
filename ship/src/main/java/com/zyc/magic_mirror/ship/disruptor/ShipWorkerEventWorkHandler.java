package com.zyc.magic_mirror.ship.disruptor;

import cn.hutool.core.date.DateUtil;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.WorkHandler;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            int sequence = shipEvent.incrementAndGet();
            logger.info("worker: "+ JsonUtil.formatJsonString(shipEvent));
            logger.info("worker check: "+ JsonUtil.formatJsonString(shipEvent.getStrategyInstance().getStrategy_context()));
            StrategyInstance strategyInstance = shipEvent.getStrategyInstance();
            if(shipEvent.getStatus().equalsIgnoreCase(ShipConst.STATUS_ERROR)){
                shipEvent.getRunPath().put(shipEvent.getStrategyInstanceId(), ShipConst.STATUS_ERROR);

                String strategyInstanceId = shipEvent.getStrategyInstanceId();
                ShipResult shipResult = new RiskShipResultImpl();
                shipResult.setStartTime(String.valueOf(DateUtil.current()));
                shipResult.setSequence(sequence);
                shipResult.setStatus(ShipConst.STATUS_ERROR);
                shipResult.setStrategyInstanceId(strategyInstanceId);
                shipResult.setStrategyName(shipEvent.getStrategyInstanceMap().get(strategyInstanceId).getStrategy_context());
                shipResult.setEndTime(String.valueOf(DateUtil.current()));
                shipResult.setMessage(shipEvent.getMsg());
                shipEvent.getShipResultMap().put(strategyInstanceId, shipResult);
            }else if(shipEvent.getStatus().equalsIgnoreCase(ShipConst.STATUS_CREATE)){
                //执行节点任务
                ShipResult shipResult = shipEvent.getShipExecutor().execute(strategyInstance);
                shipResult.setSequence(sequence);
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
            logger.error("ship worker handler error: ", e);
        }finally {
            shipEvent.getCdl().countDown();
            if(shipEvent.getCdl().getCount() == 0){
                shipEvent.getGroupCdl().countDown();
                logger.info("request_id: {}, result: {}", shipEvent.getRequestId(), JsonUtil.formatJsonString(shipEvent.getShipResultMap()));
            }
            shipEvent.clear();
        }
    }


    public ShipEvent reBuildShipEvent(ShipEvent shipEvent, String strategyId){
        ShipEvent shipEvent1 = new ShipEvent();
        shipEvent1.setSequence(shipEvent.getSequence());
        shipEvent1.setRequestId(shipEvent.getRequestId());
        shipEvent1.setLogGroupId(shipEvent.getLogGroupId());
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
        shipEvent1.setRunParam(shipEvent.getRunParam());
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
