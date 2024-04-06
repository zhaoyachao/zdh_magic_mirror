package com.zyc.ship.log;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShipOnlineRiskLog {

    public static Logger logger= LoggerFactory.getLogger(ShipOnlineRiskLog.class);

    public static void info(CommonLog commonLog){
        logger.info(JSONObject.toJSONString(commonLog));
    }

    /**
     * 用于保存入口流量信息,用于异常数据恢复
     */
    public static void putMessage2Queue(String message){
        //此处待队列实现,建议接入kafka
        logger.info(message);

    }
}
