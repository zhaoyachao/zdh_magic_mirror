package com.zyc.magic_mirror.ship.disruptor;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.zyc.magic_mirror.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * master disruptor , 用于策略实例的上下游关系,判断策略实例是否可执行
 * 为了减轻master复杂性,master废弃依赖级别校验,所有策略均采用上游成功触发
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

            logger.info("master: "+ JsonUtil.formatJsonString(shipEvent));
            String operate = shipEvent.getStrategyInstance().getOperate();
            //判定级别0：成功时运行,1:杀死时运行,2:失败时运行,默认成功时运行,3:上游执行完成后触发(不关心成功/失败)
            String dependLevel = shipEvent.getStrategyInstance().getDepend_level();
            Operate operate1 = check(operate);
            String status = ShipConst.STATUS_WAIT;
            //增加唯一锁,防止多父节点同时触发检查
            synchronized (shipEvent.getStrategyInstanceId()+shipEvent.getRequestId().intern()){
                status = operate1.execute(shipEvent.getStrategyInstanceId(), shipEvent.getDag(), shipEvent.getRunPath());
            }
            Disruptor disruptor = shipEvent.getWorkerDisruptor();
            if(status.equalsIgnoreCase(ShipConst.STATUS_ERROR)){
                ShipEvent shipEvent1 = reBuildShipEvent(shipEvent, shipEvent.getStrategyInstanceId());
                shipEvent1.setStatus(ShipConst.STATUS_ERROR);
                shipEvent1.setMsg("上游存在失败或异常");
                EventTranslator<ShipEvent> eventEventTranslator = DisruptorManager.buildByShipEvent(shipEvent1);
                if(disruptor != null){
                    disruptor.publishEvent(eventEventTranslator);
                }
            }else if(status.equalsIgnoreCase(ShipConst.STATUS_WAIT)){

            }else if(status.equalsIgnoreCase(ShipConst.STATUS_CREATE)){
                ShipEvent shipEvent1 = reBuildShipEvent(shipEvent, shipEvent.getStrategyInstanceId());
                shipEvent1.setStatus(ShipConst.STATUS_CREATE);
                EventTranslator<ShipEvent> eventEventTranslator = DisruptorManager.buildByShipEvent(shipEvent1);
                if(disruptor != null){
                    disruptor.publishEvent(eventEventTranslator);
                }
            }

        }catch (Exception e){
            logger.error("ship master handler error: ", e);
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
