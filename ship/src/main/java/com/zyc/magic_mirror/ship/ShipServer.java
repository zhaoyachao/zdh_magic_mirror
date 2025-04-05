package com.zyc.magic_mirror.ship;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zyc.magic_mirror.common.http.HttpServer;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.common.util.ServerManagerUtil;
import com.zyc.magic_mirror.common.util.SnowflakeIdWorker;
import com.zyc.magic_mirror.ship.action.ShipAction;
import com.zyc.magic_mirror.ship.common.Const;
import com.zyc.magic_mirror.ship.conf.ShipConf;
import com.zyc.magic_mirror.ship.disruptor.DisruptorManager;
import com.zyc.magic_mirror.ship.disruptor.ShipMasterEventWorkHandler;
import com.zyc.magic_mirror.ship.disruptor.ShipWorkerEventWorkHandler;
import com.zyc.magic_mirror.ship.service.impl.CacheFunctionServiceImpl;
import com.zyc.magic_mirror.ship.service.impl.CacheStrategyServiceImpl;
import com.zyc.magic_mirror.ship.util.FilterHttpUtil;
import com.zyc.magic_mirror.ship.util.LabelHttpUtil;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
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
            TimeUnit.MICROSECONDS, new LinkedBlockingDeque<>(),new ThreadFactoryBuilder().setNameFormat("ship_log_%d").build()
            );

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
                    new ThreadFactoryBuilder().setNameFormat("ship_schedule_%d").build());

            Thread schedule = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try{
                            logger.info("更新配置");
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


            HttpServer httpServer = new HttpServer();
            ShipAction shipAction = new ShipAction();

            httpServer.registerAction(shipAction.getUri(), shipAction);

            httpServer.start(properties);

        }catch (Exception e){
            logger.error("ship server error: ", e);
            System.exit(-1);
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
                            List<Map<String, Object>> jsonArray = JsonUtil.toJavaListMap(riskLogStr.toString());
                            if(jsonArray != null && jsonArray.size()>0){
                                String requestId = jsonArray.get(0).get("requestId").toString();
                                //截取前10位
                                String task_log_id=requestId;
                                // strategyGroupInstanceId=task_log_id
                                for(Object obj: jsonArray){
                                    String job_id= ((Map<String, Object>)obj).get("strategyGroupInstanceId").toString();
                                    LogUtil.info(job_id, task_log_id, JsonUtil.formatJsonString(obj));
                                }

                            }
                        }
                        RQueueClient rQueueClient2 = RQueueManager.getRQueueClient(Const.SHIP_ONLINE_MANAGER_LOG_QUEUE);

                        Object managerLogStr = rQueueClient2.poll();
                        if(managerLogStr != null){
                            logger.info(managerLogStr.toString());

                            List<Map<String, Object>> jsonArray = JsonUtil.toJavaListMap(managerLogStr.toString());
                            if(jsonArray != null && jsonArray.size()>0){
                                String requestId = jsonArray.get(0).get("requestId").toString();
                                //截取前10位
                                String task_log_id=requestId;
                                // strategyGroupInstanceId=task_log_id
                                for(Map<String, Object> obj: jsonArray){
                                    String job_id= obj.get("strategyGroupInstanceId").toString();
                                    LogUtil.info(job_id, task_log_id, JsonUtil.formatJsonString(obj));
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
