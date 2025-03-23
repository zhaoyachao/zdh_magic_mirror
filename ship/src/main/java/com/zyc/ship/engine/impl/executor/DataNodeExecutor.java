package com.zyc.ship.engine.impl.executor;

import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DataNodeExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(DataNodeExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String data_node, String uid){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点
            String s_data_node = run_jsmind_data.getOrDefault("data_node","").toString();
            if(data_node.equalsIgnoreCase(s_data_node)){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }else{
                shipResult.setMessage("未命中数据节点: "+s_data_node);
            }
        }catch (Exception e){
            logger.error("ship excutor datanode error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
