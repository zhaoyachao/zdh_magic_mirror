package com.zyc.label.calculate.impl;

import com.google.gson.Gson;
import com.zyc.common.queue.QueueHandler;
import com.zyc.queue.Consumer;
import com.zyc.queue.client.ConsumerHandlerImpl;
import com.zyc.queue.client.ConsumerImpl;
import com.zyc.queue.core.QueueDataInfo;

import java.util.Map;
import java.util.Properties;

public class ZQueueHandler implements QueueHandler {

    private Properties properties;

    private Consumer consumer=null;

    @Override
    public Map<String, Object> handler() {
        try{
            if(consumer == null){
                synchronized (this){
                    if(consumer == null){
                        initConsumer(properties);
                    }
                }
            }


            Object o=consumer.poll();
            if(o != null) {
                QueueDataInfo queueDataInfo = (QueueDataInfo) o;
                //可在此处处理数据
                Gson gson = new Gson();
                //此处需要字符串转换,因2次json导致json串转换2次,解析异常

                queueDataInfo.setMsg(queueDataInfo.getMsg().replaceAll("\"\\{", "\\{"));
                queueDataInfo.setMsg(queueDataInfo.getMsg().replaceAll("}\"", "}"));
                System.out.println(queueDataInfo.getMsg());
                Map m = gson.fromJson(queueDataInfo.getMsg(), Map.class);
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

    public Consumer initConsumer(Properties queueConfig) throws Exception {
        try{
            String host = queueConfig.getProperty("queue.server.host", "127.0.0.1");
            int port = Integer.valueOf(queueConfig.getProperty("queue.server.port", "9000"));
            String queue = queueConfig.getProperty("queue.server.queue", "LABEL");
            ConsumerImpl consumer=new ConsumerImpl();
            consumer.init(host,port);
            consumer.setConsumerHandler(new ConsumerHandlerImpl());
            if(!consumer.is_connect(5)){
                throw new Exception("链接队列失败");
            }
            consumer.setQueue(queue);
            this.consumer=consumer;
            return consumer;
        }catch (Exception e){
            throw new Exception("初始化队列消费者异常,", e.getCause());
        }

    }
}
