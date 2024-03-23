package com.zyc.label.calculate.impl;

import com.google.gson.Gson;
import com.zyc.common.queue.QueueHandler;
import com.zyc.rqueue.RQueueManager;

import java.util.Map;
import java.util.Properties;

/**
 * 暂留坑位
 */
public class ZQueueHandler implements QueueHandler {

    private Properties properties;

    @Override
    public Map<String, Object> handler() {
        try{
            String queueName = properties.getProperty("rqueue.name", "rqueue_label");
            Object o= RQueueManager.getRQueueClient(queueName).poll();
            if(o != null) {
                //可在此处处理数据
                Gson gson = new Gson();
                //此处需要字符串转换,因2次json导致json串转换2次,解析异常

                Map m = gson.fromJson(o.toString(), Map.class);
                return m;
            }else{
                return null;
            }
        }catch (Exception e){

        }

        return null;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties=properties;
    }

}
