package com.zyc.ship;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.ServerManagerUtil;
import com.zyc.common.util.SnowflakeIdWorker;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import com.zyc.ship.common.Const;
import com.zyc.ship.conf.ShipConf;
import com.zyc.ship.disruptor.DisruptorManager;
import com.zyc.ship.disruptor.ShipMasterEventWorkHandler;
import com.zyc.ship.disruptor.ShipWorkerEventWorkHandler;
import com.zyc.ship.netty.NettyServer;
import com.zyc.ship.service.impl.CacheFunctionServiceImpl;
import com.zyc.ship.service.impl.CacheStrategyServiceImpl;
import com.zyc.ship.util.FilterHttpUtil;
import com.zyc.ship.util.LabelHttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ship server
 */
public class ShipServer {
    public static Logger logger= LoggerFactory.getLogger(ShipServer.class);

    public static ThreadPoolExecutor consumerLogThreadPoolExecutor = new ThreadPoolExecutor(1,
            1, 1000*60*60,
            TimeUnit.MICROSECONDS, new LinkedBlockingDeque<>(),
            Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

    public static void main(String[] args) {

        try{
            String conf_path = ShipServer.class.getClassLoader().getResource("application.properties").getPath();

            Properties properties = new Properties();
            InputStream inputStream= ShipServer.class.getClassLoader().getResourceAsStream("application.properties");
            properties.load(inputStream);
            File confFile = new File("conf/application.properties");
            if(confFile.exists()){
                conf_path = confFile.getPath();
                inputStream = new FileInputStream(confFile);
                properties.load(inputStream);
            }
            logger.info("加载配置文件路径:{}", conf_path);

            ShipConf.setConf(properties);
            SnowflakeIdWorker.init(Integer.valueOf(properties.getProperty("work.id", "1")),
                    Integer.valueOf(properties.getProperty("data.center.id", "1"))
                    );
            JedisPoolUtil.connect(properties);

            String serviceName = properties.getProperty("service.name");
            ServerManagerUtil.registerServiceName(serviceName);
            ServerManagerUtil.ServiceInstanceConf serviceInstanceConf = ServerManagerUtil.registerServiceInstance(serviceName);

            initRQueue(properties);
            LabelHttpUtil.init(properties);
            FilterHttpUtil.init(properties);
            CacheStrategyServiceImpl cacheStrategyService = new CacheStrategyServiceImpl();
            CacheFunctionServiceImpl cacheFunctionService = new CacheFunctionServiceImpl();
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,
                    1, 1000*60*60,
                    TimeUnit.MICROSECONDS, new LinkedBlockingDeque<>(),
                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

            Thread schedule = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try{
                            logger.debug("更新配置");
                            cacheStrategyService.schedule();
                            cacheFunctionService.schedule();
                            Thread.sleep(1000*60);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }

                }
            });
            schedule.setName("ship_schedule");
            threadPoolExecutor.execute(schedule);

            consumerLog(serviceInstanceConf);
            //初始化disruptor
            int masterHandlerNum = Integer.valueOf(properties.getProperty("ship.disruptr.master.handler.num", "1"));
            int workerHandlerNum = Integer.valueOf(properties.getProperty("ship.disruptr.worker.handler.num", "1"));
            int masterRingBufferSize = Integer.valueOf(properties.getProperty("ship.disruptr.master.ring.num", "1024*1024"));
            int workerRingBufferSize = Integer.valueOf(properties.getProperty("ship.disruptr.worker.ring.num", "1024*1024"));
            DisruptorManager.getDisruptor("ship_master", masterHandlerNum, new ShipMasterEventWorkHandler(), masterRingBufferSize);
            DisruptorManager.getDisruptor("ship_worker", workerHandlerNum, new ShipWorkerEventWorkHandler(), workerRingBufferSize);

            NettyServer nettyServer=new NettyServer();
            nettyServer.start(properties);

        }catch (Exception e){
            logger.error("ship server error: ", e);
        }

    }

    /**
     * 初始化分布式优先级队列
     * @param properties
     */
    public static void initRQueue(Properties properties){
        String host = properties.getProperty("redis.host");
        String auth = properties.getProperty("redis.password");
        String port = properties.getProperty("redis.port");
        if(properties.getProperty("redis.mode", "single").equalsIgnoreCase("cluster")){
            RQueueManager.buildDefault(host, auth);
        }else{
            RQueueManager.buildDefault(host+":"+port, auth);
        }

    }

    /**
     * 经营/风控 实时日志记录
     * 当前功能未实现,只是做一个消费逻辑,后续可扩展
     * @param serviceInstanceConf
     */
    public static void consumerLog(ServerManagerUtil.ServiceInstanceConf serviceInstanceConf){

        Runnable task = new Runnable(){

            @Override
            public void run() {

                while (true){
                    try{

                        ServerManagerUtil.heartbeatReport(serviceInstanceConf);
                        ServerManagerUtil.checkServiceRunningMode(serviceInstanceConf);

                        RQueueClient rQueueClient = RQueueManager.getRQueueClient(Const.SHIP_ONLINE_RISK_LOG_QUEUE);

                        Object riskLogStr = rQueueClient.poll();

                        if(riskLogStr != null){
                            logger.info(riskLogStr.toString());
                            JSONArray jsonArray = JSON.parseArray(riskLogStr.toString());
                            if(jsonArray != null && jsonArray.size()>0){
                                String requestId = jsonArray.getJSONObject(0).getString("requestId");
                                //截取前10位
                                String task_log_id=requestId;
                                // strategyGroupInstanceId=task_log_id
                                for(Object obj: jsonArray){
                                    String job_id= ((JSONObject)obj).getString("strategyGroupInstanceId");
                                    LogUtil.info(job_id, task_log_id, ((JSONObject)obj).toJSONString());
                                }

                            }
                        }
                        RQueueClient rQueueClient2 = RQueueManager.getRQueueClient(Const.SHIP_ONLINE_MANAGER_LOG_QUEUE);

                        Object managerLogStr = rQueueClient2.poll();
                        if(managerLogStr != null){
                            logger.info(managerLogStr.toString());

                            JSONArray jsonArray = JSON.parseArray(managerLogStr.toString());
                            if(jsonArray != null && jsonArray.size()>0){
                                String requestId = jsonArray.getJSONObject(0).getString("requestId");
                                //截取前10位
                                String task_log_id=requestId;
                                // strategyGroupInstanceId=task_log_id
                                for(Object obj: jsonArray){
                                    String job_id= ((JSONObject)obj).getString("strategyGroupInstanceId");
                                    LogUtil.info(job_id, task_log_id, ((JSONObject)obj).toJSONString());
                                }

                            }
                        }

                        if(riskLogStr == null && managerLogStr == null){
                            Thread.sleep(1000);
                        }

                    }catch (Exception e){

                    }

                }
            }
        };


        consumerLogThreadPoolExecutor.submit(task);
    }
}
