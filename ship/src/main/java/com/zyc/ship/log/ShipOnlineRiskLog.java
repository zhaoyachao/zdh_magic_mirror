package com.zyc.ship.log;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShipOnlineRiskLog {

    public static Logger logger= LoggerFactory.getLogger(ShipOnlineRiskLog.class);

    public static void info(CommonLog commonLog){
        logger.info(JSONObject.toJSONString(commonLog));
    }
}
