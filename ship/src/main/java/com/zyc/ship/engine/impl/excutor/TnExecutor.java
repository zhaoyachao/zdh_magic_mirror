package com.zyc.ship.engine.impl.excutor;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import com.zyc.rqueue.RQueueMode;
import com.zyc.ship.common.Const;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TnExecutor {
    private static Logger logger= LoggerFactory.getLogger(TnExecutor.class);

    public ShipResult execute(JSONObject run_jsmind_data, String uid, StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            //1 获取对比时间类型,相对或者绝对
            String tn_type = run_jsmind_data.getString("tn_type");
            String tn_unit = run_jsmind_data.getString("tn_unit");
            String tn_value = run_jsmind_data.getString("tn_value");
            if(StringUtils.isEmpty(tn_value)){
                throw new Exception("tn模块时间参数不可为空");
            }
            Timestamp executeTime = null;
            long current = System.currentTimeMillis();
            if(tn_type.equalsIgnoreCase(Const.TN_TYPE_RELATIVE)){
                //相对时间

                if(StringUtils.isEmpty(tn_unit)){
                    throw new Exception("tn模块设置相对时间单位为空");
                }
                if(!NumberUtil.isInteger(tn_value)){
                    throw new Exception("tn模块设置相对时间必须是整数");
                }

                if(tn_unit.equalsIgnoreCase("minute")){
                    executeTime =DateUtil.offsetMinute(new Date(), Integer.valueOf(tn_value)).toTimestamp();
                }else if(tn_unit.equalsIgnoreCase("hour")){
                    executeTime =DateUtil.offsetHour(new Date(), Integer.valueOf(tn_value)).toTimestamp();
                }else if(tn_unit.equalsIgnoreCase("day")){
                    executeTime =DateUtil.offsetDay(new Date(), Integer.valueOf(tn_value)).toTimestamp();
                }

            }else if(tn_type.equalsIgnoreCase(Const.TN_TYPE_ABSOLUTE)){

                //绝对时间
                String[] values = tn_value.split(";");
                if(values.length == 1){
                    //起始结束一样
                    executeTime = DateUtil.parse(values[0], "yyyy-MM-dd HH:mm:ss").toTimestamp();
                }
            }

            //计算延迟时间差
            long delay_senond = executeTime.getTime() - current;

            String queueName = "ship_tn_queue"+"_"+ DateUtil.format(executeTime, "yyyyMMddHH");

            //获取当前执行中间结果,生成延迟队列数据
            JSONObject jsonObject = JSON.parseObject(JSON.toJSONString(strategyInstance));
            jsonObject.put("request_id", shipEvent.getRequestId());
            jsonObject.put("input", shipEvent.getInputParam());

            //获取当前request_id
            RQueueClient<String> rQueueClient = RQueueManager.getRQueueClient(queueName, RQueueMode.DELAYEDQUEUE);
            rQueueClient.offer(jsonObject.toJSONString(), delay_senond, TimeUnit.SECONDS);
        }catch (Exception e){
            logger.error("ship excutor tn error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
