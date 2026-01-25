package com.zyc.magic_mirror.ship;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zyc.magic_mirror.common.http.HttpAction;
import com.zyc.magic_mirror.common.http.HttpServer;
import com.zyc.magic_mirror.common.http.PackageScanner;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.*;
import com.zyc.magic_mirror.ship.common.Const;
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

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * ship server
 */
public class ShipServer {
    public static final Logger logger = LoggerFactory.getLogger(ShipServer.class);

    private static final List<String> ACTION_PACKAGES = Lists.newArrayList("com.zyc.magic_mirror.ship.action");
    
    // 日志消费线程池 - 优化配置
    public static final ThreadPoolExecutor consumerLogThreadPoolExecutor = new ThreadPoolExecutor(
            1, 1, 60, TimeUnit.SECONDS, 
            new LinkedBlockingDeque<>(1000), 
            new ThreadFactoryBuilder().setNameFormat("ship_log_%d").build()
    );

    public static void main(String[] args) {
        try {
            LogIdUtil.generateAndSet();
            logger.info("Ship server starting...");
            // 加载配置文件
            ConfigUtil.load();
            
            // 初始化系统组件
            initSystemComponents();
            
            // 初始化服务注册
            String serviceName = ConfigUtil.get(ConfigUtil.SERVICE_NAME);
            ServerManagerUtil.registerServiceName(serviceName);
            ServerManagerUtil.ServiceInstanceConf serviceInstanceConf = ServerManagerUtil.registerServiceInstance(serviceName);
            
            // 初始化业务组件
            initBusinessComponents(ConfigUtil.getConfig(), serviceInstanceConf);
            
            // 启动HTTP服务器
            startHttpServer(ConfigUtil.getConfig());
            
            logger.info("Ship server started successfully!");
            
        } catch (Exception e) {
            logger.error("Ship server startup error: ", e);
            System.exit(-1);
        }finally {
            LogIdUtil.clear();
        }
    }
    
    /**
     * 初始化系统组件
     */
    private static void initSystemComponents() {
        // 初始化日志类型
        initLogType();
        
        // 初始化ID生成器
        SnowflakeIdWorker.init(
                Integer.valueOf(ConfigUtil.get("work.id", "1")),
                Integer.valueOf(ConfigUtil.get("data.center.id", "1"))
        );

        // 连接Redis
        JedisPoolUtil.connect(ConfigUtil.getConfig());
    }
    
    /**
     * 初始化业务组件
     */
    private static void initBusinessComponents(Properties properties, ServerManagerUtil.ServiceInstanceConf serviceInstanceConf) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        // 初始化RQueue
        initRQueue(properties);
        
        // 初始化HTTP工具
        LabelHttpUtil.init(properties);
        FilterHttpUtil.init(properties);
        
        // 初始化定时任务
        initScheduledTasks();
        
        // 初始化Disruptor
        initDisruptor(properties);
        
        // 启动日志消费
        consumerLog(serviceInstanceConf);
        
        // 执行初始化优化
        optimize();
    }
    
    /**
     * 初始化定时任务
     */
    private static void initScheduledTasks() {
        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(
                1, 
                new ThreadFactoryBuilder().setNameFormat("ship_schedule_%d").build()
        );
        
        CacheStrategyServiceImpl cacheStrategyService = new CacheStrategyServiceImpl();
        CacheFunctionServiceImpl cacheFunctionService = new CacheFunctionServiceImpl();
        
        // 每分钟执行一次更新配置任务
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LogIdUtil.generateAndSet();
                logger.info("更新配置");
                cacheStrategyService.schedule();
                cacheFunctionService.schedule();
            } catch (Exception e) {
                logger.error("定时更新配置失败: ", e);
            }finally {
                LogIdUtil.clear();
            }
        }, 0, 1, TimeUnit.MINUTES);
        
        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }));
    }
    
    /**
     * 初始化Disruptor
     */
    private static void initDisruptor(Properties properties) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        int masterHandlerNum = Integer.valueOf(properties.getProperty("ship.disruptr.master.handler.num", "1"));
        int workerHandlerNum = Integer.valueOf(properties.getProperty("ship.disruptr.worker.handler.num", "1"));
        int masterRingBufferSize = Integer.valueOf(properties.getProperty("ship.disruptr.master.ring.num", "1024"));
        int workerRingBufferSize = Integer.valueOf(properties.getProperty("ship.disruptr.worker.ring.num", "1024"));
        
        DisruptorManager.getDisruptor("ship_master", masterHandlerNum, new ShipMasterEventWorkHandler(), masterRingBufferSize);
        DisruptorManager.getDisruptor("ship_worker", workerHandlerNum, new ShipWorkerEventWorkHandler(), workerRingBufferSize);
    }
    
    /**
     * 启动HTTP服务器
     */
    private static void startHttpServer(Properties properties) throws Exception {
        // 自动扫描并注册HTTP Action
        for(String packageName : ACTION_PACKAGES){
            PackageScanner.autoInit(packageName, HttpAction.class);
        }
        HttpServer httpServer = new HttpServer();
        httpServer.start(properties);
    }

    /**
     * 执行初始化优化
     */
    public static void optimize() {
        try {
            // 可以在这里添加实际需要的初始化优化代码
            logger.info("执行初始化优化");
        } catch (Exception e) {
            logger.error("初始化优化失败: ", e);
        }
    }

    /**
     * 初始化分布式优先级队列
     */
    public static void initRQueue(Properties properties) {
        String host = properties.getProperty("redis.host");
        String auth = properties.getProperty("redis.password");
        String port = properties.getProperty("redis.port");
        
        if (properties.getProperty("redis.mode", "single").equalsIgnoreCase("cluster")) {
            RQueueManager.buildDefault(host, auth);
        } else {
            RQueueManager.buildDefault(host + ":" + port, auth);
        }
    }

    /**
     * 经营/风控 实时日志记录
     * 当前功能未实现,只是做一个消费逻辑,后续可扩展
     */
    public static void consumerLog(ServerManagerUtil.ServiceInstanceConf serviceInstanceConf) {
        Runnable task = () -> {
            while (true) {
                try {
                    LogIdUtil.generateAndSet();
                    // 更新服务实例信息
                    ServerManagerUtil.registerServiceInstance(serviceInstanceConf.getService_name());
                    ServerManagerUtil.heartbeatReport(serviceInstanceConf);
                    ServerManagerUtil.checkServiceRunningMode(serviceInstanceConf);

                    // 处理风控日志
                    processRiskLogs();
                    
                    // 处理经营日志
                    processManagerLogs();
                    
                    // 无日志时短暂休眠
                    Thread.sleep(100);
                    
                } catch (InterruptedException e) {
                    logger.error("日志消费线程被中断: ", e);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("日志消费异常: ", e);
                    // 避免异常导致循环过快
                    try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                } finally {
                    LogIdUtil.clear();
                }
            }
        };
        
        consumerLogThreadPoolExecutor.submit(task);
    }
    
    /**
     * 处理风险日志
     */
    private static void processRiskLogs() throws Exception {
        RQueueClient rQueueClient = RQueueManager.getRQueueClient(Const.SHIP_ONLINE_RISK_LOG_QUEUE);
        Object riskLogStr = rQueueClient.poll();

        if (riskLogStr != null) {
            logger.info(riskLogStr.toString());
            try {
                List<Map<String, Object>> jsonArray = JsonUtil.toJavaListMap(riskLogStr.toString());
                if (jsonArray != null && !jsonArray.isEmpty()) {
                    Map<String, Object> firstLog = jsonArray.get(0);
                    if (firstLog.containsKey("requestId")) {
                        String requestId = firstLog.get("requestId").toString();
                        String taskLogId = requestId; // 可以根据需要截取
                        
                        for (Map<String, Object> logObj : jsonArray) {
                            String jobId = logObj.getOrDefault("strategyGroupInstanceId", taskLogId).toString();
                            LogUtil.info(jobId, taskLogId, JsonUtil.formatJsonString(logObj));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("处理风控日志失败: ", e);
            }
        }
    }
    
    /**
     * 处理管理日志
     */
    private static void processManagerLogs() throws Exception {
        RQueueClient rQueueClient = RQueueManager.getRQueueClient(Const.SHIP_ONLINE_MANAGER_LOG_QUEUE);
        Object managerLogStr = rQueueClient.poll();

        if (managerLogStr != null) {
            logger.info(managerLogStr.toString());
            try {
                List<Map<String, Object>> jsonArray = JsonUtil.toJavaListMap(managerLogStr.toString());
                if (jsonArray != null && !jsonArray.isEmpty()) {
                    Map<String, Object> firstLog = jsonArray.get(0);
                    if (firstLog.containsKey("requestId")) {
                        String requestId = firstLog.get("requestId").toString();
                        String taskLogId = requestId;
                        
                        for (Map<String, Object> logObj : jsonArray) {
                            String jobId = logObj.getOrDefault("strategyGroupInstanceId", taskLogId).toString();
                            LogUtil.info(jobId, taskLogId, JsonUtil.formatJsonString(logObj));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("处理经营日志失败: ", e);
            }
        }
    }

    /**
     * 初始化日志类型
     */
    public static void initLogType() {
        String logType = ConfigUtil.get(ConfigUtil.LOG_TYPE, com.zyc.magic_mirror.common.util.Const.LOG_TYPE_MYSQL);
        LogUtil.logType = logType;

        if (logType.equalsIgnoreCase(com.zyc.magic_mirror.common.util.Const.LOG_TYPE_MONGODB)) {
            String mongodbUrl = ConfigUtil.get(ConfigUtil.LOG_MONGODB_URL, "mongodb://localhost:27017");
            String mongodbDb = ConfigUtil.get(ConfigUtil.LOG_MONGODB_DB, "zdh");
            Integer maxPoolSize = Integer.valueOf(ConfigUtil.get(ConfigUtil.LOG_MONGODB_MAX_POOL_SIZE, "1"));
            Integer minPoolSize = Integer.valueOf(ConfigUtil.get(ConfigUtil.LOG_MONGODB_MIN_POOL_SIZE, "1"));
            Integer maxWaitTime = Integer.valueOf(ConfigUtil.get(ConfigUtil.LOG_MONGODB_MAX_WAIT_TIME, "5"));
            LogUtil.initMongoDb(mongodbUrl, mongodbDb, maxPoolSize, minPoolSize, maxWaitTime);
        }
    }
}