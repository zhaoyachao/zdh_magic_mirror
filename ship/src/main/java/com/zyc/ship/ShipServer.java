package com.zyc.ship;

import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.SnowflakeIdWorker;
import com.zyc.rqueue.RQueueManager;
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

/**
 * ship server
 */
public class ShipServer {
    public static Logger logger= LoggerFactory.getLogger(ShipServer.class);


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

            SnowflakeIdWorker.init(Integer.valueOf(properties.getProperty("work.id", "1")),
                    Integer.valueOf(properties.getProperty("data.center.id", "1"))
                    );
            JedisPoolUtil.connect(properties);

            initRQueue(properties);
            LabelHttpUtil.init(properties);
            FilterHttpUtil.init(properties);
            CacheStrategyServiceImpl cacheStrategyService = new CacheStrategyServiceImpl();
            CacheFunctionServiceImpl cacheFunctionService = new CacheFunctionServiceImpl();
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
            schedule.start();

            //初始化disruptor
            int masterHandlerNum = Integer.valueOf(properties.getProperty("ship.disruptr.master.handler.num", "1"));
            int workerHandlerNum = Integer.valueOf(properties.getProperty("ship.disruptr.worker.handler.num", "1"));
            DisruptorManager.getDisruptor("ship_master", masterHandlerNum, new ShipMasterEventWorkHandler());
            DisruptorManager.getDisruptor("ship_worker", workerHandlerNum, new ShipWorkerEventWorkHandler());

            NettyServer nettyServer=new NettyServer();
            nettyServer.start(properties);

        }catch (Exception e){
            e.printStackTrace();
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

}
