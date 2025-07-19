package com.zyc.magic_mirror.ship.engine.impl.executor;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.common.Const;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import com.zyc.magic_mirror.ship.entity.ShipCommonInputParam;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import com.zyc.rqueue.RQueueMode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 实时场景-tn, 不满足返回error
 * 当第一次来时,计算tn时间,并写入未来执行时间,同时投递的延迟队列
 * 延迟队列触发后,会再次走ship决策
 */
public class TnExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(TnExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String uid, StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{

            //获取是否有可执行时间,无则进行本次判断,有则判断时间是否可执行

            String param = ((ShipCommonInputParam)shipEvent.getInputParam()).getParam();
            Map<String, Object> params = JsonUtil.toJavaMap(param);
            if(params.containsKey(com.zyc.magic_mirror.common.util.Const.STRATEGY_INSTANCE_DOUBLECHECK_TIME)){
                // 满足则执行
                if(Long.valueOf(params.get(com.zyc.magic_mirror.common.util.Const.STRATEGY_INSTANCE_DOUBLECHECK_TIME).toString()) < System.currentTimeMillis()){

                }else{
                    tmp = ShipResultStatusEnum.ERROR.code;
                }
                shipResult.setStatus(tmp);
                return shipResult;
            }

            //1 获取对比时间类型,相对或者绝对
            String tn_type = run_jsmind_data.getOrDefault("tn_type", "").toString();
            String tn_unit = run_jsmind_data.getOrDefault("tn_unit", "").toString();
            String tn_value = run_jsmind_data.getOrDefault("tn_value", "").toString();
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

            //参数增加未来之星时间
            params.put(com.zyc.magic_mirror.common.util.Const.STRATEGY_INSTANCE_DOUBLECHECK_TIME, executeTime.getTime());
            //获取当前执行中间结果,生成延迟队列数据
            Map<String, Object> jsonObject = JsonUtil.toJavaMap(JsonUtil.formatJsonString(strategyInstance));
            jsonObject.put("request_id", shipEvent.getRequestId());
            jsonObject.put("input", JsonUtil.formatJsonString(params));

            //获取当前request_id
            RQueueClient<String> rQueueClient = RQueueManager.getRQueueClient(queueName, RQueueMode.DELAYEDQUEUE);
            rQueueClient.offer(JsonUtil.formatJsonString(jsonObject), delay_senond, TimeUnit.SECONDS);
            tmp = ShipResultStatusEnum.ERROR.code;
        }catch (Exception e){
            logger.error("ship excutor tn error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
