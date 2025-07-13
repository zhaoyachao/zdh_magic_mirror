package com.zyc.magic_mirror.ship.seaport.impl;

import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.common.Const;
import com.zyc.magic_mirror.ship.engine.Engine;
import com.zyc.magic_mirror.ship.engine.impl.ShipOnLineManagerEngine;
import com.zyc.magic_mirror.ship.engine.impl.ShipOnLineRiskEngine;
import com.zyc.magic_mirror.ship.entity.InputParam;
import com.zyc.magic_mirror.ship.entity.OutputParam;
import com.zyc.magic_mirror.ship.entity.ShipBaseOutputParam;
import com.zyc.magic_mirror.ship.entity.ShipCommonInputParam;
import com.zyc.magic_mirror.ship.exception.ErrorCode;
import com.zyc.magic_mirror.ship.log.ShipOnlineRiskLog;
import com.zyc.magic_mirror.ship.seaport.Input;
import com.zyc.magic_mirror.ship.service.impl.CacheStrategyServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ship server 流量入口-接收流量,获取相应执行引擎,并输出结果
 */
public class ShipInput implements Input {
    private Logger logger= LoggerFactory.getLogger(this.getClass());

    @Override
    public OutputParam accept(InputParam inputParam){
        try{
            //提前写入队列/日志用于后期数据恢复
            ShipOnlineRiskLog.putMessage2Queue(JsonUtil.formatJsonString(inputParam));
            //接入流量,以用户为参数
            ShipCommonInputParam shipCommonInputParam = (ShipCommonInputParam) inputParam;
            //校验参数
            shipCommonInputParam.checkParams();

            //根据scene获取执行引擎
            Engine engine=getEngine(shipCommonInputParam.getScene(), shipCommonInputParam);

            //执行引擎返回结果
            OutputParam outputParam = engine.execute();
            return outputParam;
        }catch (Exception e){
            logger.error(e.getMessage());
            return new ShipBaseOutputParam(ErrorCode.PARAM_CHECK_ERROR_CODE, e.getMessage());
        }
    }

    public Engine getEngine(String scene, InputParam inputParam){

        if(scene.equalsIgnoreCase(Const.ONLINE_MANAGER)){
            return new ShipOnLineManagerEngine(inputParam, new CacheStrategyServiceImpl());
        }else if(scene.equalsIgnoreCase(Const.ONLINE_RISK)){
            return new ShipOnLineRiskEngine(inputParam, new CacheStrategyServiceImpl());
        }
        return null;
    }

}
