package com.zyc.ship.disruptor;

import com.alibaba.fastjson.JSON;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * master disruptor , 用于策略实例的上下游关系,判断策略实例是否可执行
 */
public class ShipMasterEventWorkHandler implements WorkHandler<ShipEvent> {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    private String handlerId;

    public ShipMasterEventWorkHandler(){
    }

    public ShipMasterEventWorkHandler(String handlerId){
        this.handlerId = handlerId;
    }

    @Override
    public void onEvent(ShipEvent shipEvent) throws Exception {
        try{

            if(shipEvent.getStopFlag().getFlag()){
                shipEvent.getRunPath().put(shipEvent.getStrategyInstanceId(), ShipConst.STATUS_ERROR);
                throw new Exception("request stop flag");
            }

            logger.info("master: "+JSON.toJSONString(shipEvent));
            String operate = shipEvent.getStrategyInstance().getOperate();
            Operate operate1 = check(operate);
            String status = operate1.execute(shipEvent.getStrategyInstanceId(), shipEvent.getDag(), shipEvent.getRunPath());
            Disruptor disruptor = shipEvent.getWorkerDisruptor();
            if(status.equalsIgnoreCase(ShipConst.STATUS_ERROR)){
                ShipEvent shipEvent1 = reBuildShipEvent(shipEvent, shipEvent.getStrategyInstanceId());
                shipEvent1.setStatus(ShipConst.STATUS_ERROR);
                EventTranslator<ShipEvent> eventEventTranslator = DisruptorManager.buildByShipEvent(shipEvent1);
                if(disruptor != null){
                    disruptor.publishEvent(eventEventTranslator);
                }
                //shipEvent.getCdl().countDown();
            }else if(status.equalsIgnoreCase(ShipConst.STATUS_WAIT)){

            }else if(status.equalsIgnoreCase(ShipConst.STATUS_CREATE)){
                ShipEvent shipEvent1 = reBuildShipEvent(shipEvent, shipEvent.getStrategyInstanceId());
                shipEvent1.setStatus(ShipConst.STATUS_CREATE);
                EventTranslator<ShipEvent> eventEventTranslator = DisruptorManager.buildByShipEvent(shipEvent1);
                if(disruptor != null){
                    disruptor.publishEvent(eventEventTranslator);
                }
                //shipEvent.getCdl().countDown();
            }
        }catch (Exception e){
            e.printStackTrace();
            long count = shipEvent.getCdl().getCount();
            while (count > 0){
                shipEvent.getCdl().countDown();
                count = shipEvent.getCdl().getCount();
            }
            if(shipEvent.getCdl().getCount() == 0){
                shipEvent.getGroupCdl().countDown();
            }
        }finally {
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
