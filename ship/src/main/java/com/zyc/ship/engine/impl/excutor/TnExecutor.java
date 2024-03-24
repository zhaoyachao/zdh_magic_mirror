package com.zyc.ship.engine.impl.excutor;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;

import java.util.Date;

public class TnExecutor {

    public String execute(JSONObject run_jsmind_data, String uid){
        String tmp = "error";
        try{
            //计算具体延迟时间
            long delaySecond = 0;
            DateTime dateTime = DateUtil.offsetSecond(new Date(), (int)delaySecond);
            String queueName = "ship_tn_queue"+"_"+ DateUtil.format(dateTime, "yyyyMMddHH");
            //RQueueClient rQueueClient = RQueueManager.getRQueueClient(queueName, RQueueMode.DELAYEDQUEUE);
            //rQueueClient.offer(run_jsmind_data, delaySecond, TimeUnit.SECONDS);
            //此处需要新增skip状态,待开发
            return tmp;
        }catch (Exception e){

        }
        return tmp;
    }
}
