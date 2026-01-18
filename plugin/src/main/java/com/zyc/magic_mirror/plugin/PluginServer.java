package com.zyc.magic_mirror.plugin;

import cn.hutool.core.util.NumberUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zyc.magic_mirror.common.entity.InstanceType;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.queue.QueueHandler;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.*;
import com.zyc.magic_mirror.plugin.calculate.impl.*;
import com.zyc.magic_mirror.plugin.impl.StrategyInstanceServiceImpl;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 插件处理主类
 */
public class PluginServer {

    private static final Logger logger = LoggerFactory.getLogger(PluginServer.class);
    private static final int DEFAULT_TASK_MAX_NUM = 50;
    private static final int DEFAULT_SLOT_NUM = 100;
    
    public static Map<String, Future<?>> tasks = new ConcurrentHashMap<>();
    
    public static void main(String[] args) {
        logger.info("插件处理服务启动...");
        try {
            // 加载配置
            ConfigUtil.load();
            
            // 初始化系统
            initSystem();
            
            // 注册服务
            String serviceName = ConfigUtil.get(ConfigUtil.SERVICE_NAME);
            AtomicInteger taskCount = new AtomicInteger(0);
            String instanceId = registerService(serviceName, ConfigUtil.getConfig(), taskCount);
            
            // 初始化任务队列和线程池
            int taskLimit = Integer.parseInt(ConfigUtil.get(ConfigUtil.TASK_MAX_NUM, String.valueOf(DEFAULT_TASK_MAX_NUM)));
            QueueHandler queueHandler = initQueueHandler(ConfigUtil.getConfig());
            ThreadPoolExecutor threadPoolExecutor = initThreadPool();
            
            // 提交监控任务
            submitMonitorTasks(threadPoolExecutor, ConfigUtil.getConfig(), taskCount);
            
            // 启动任务消费循环
            startTaskConsumptionLoop(threadPoolExecutor, queueHandler, ConfigUtil.getConfig(), taskCount, taskLimit, instanceId);
            
        } catch (Exception e) {
            logger.error("插件处理服务启动失败: ", e);
            System.exit(1);
        }
    }

    /**
     * 初始化系统组件
     */
    private static void initSystem() throws Exception {
        initLogType(ConfigUtil.getConfig());
        checkConfig(ConfigUtil.getConfig());
        JedisPoolUtil.connect(ConfigUtil.getConfig());
        loadIdMappingConf(ConfigUtil.getConfig());
        loadFilterConf(ConfigUtil.getConfig());
    }
    
    /**
     * 注册服务实例
     */
    private static String registerService(String serviceName, Properties config, AtomicInteger taskCount) {
        ServerManagerUtil.registerServiceName(serviceName);
        ServerManagerUtil.ServiceInstanceConf serviceInstanceConf = ServerManagerUtil.registerServiceInstance(serviceName);
        serviceInstanceConf.setAtomicInteger(taskCount);
        
        String slotNum = config.getProperty("task.slot.total.num", "0");
        String slot = config.getProperty("task.slot", "-1,-1");
        String instanceId = ServerManagerUtil.buildServiceInstance();
        ServerManagerUtil.reportSlot(instanceId, slotNum, slot);
        
        logger.info("服务注册成功，实例ID: {}", instanceId);
        return instanceId;
    }
    
    /**
     * 初始化任务队列处理器
     */
    private static QueueHandler initQueueHandler(Properties config) {
        QueueHandler queueHandler = new DbQueueHandler();
        queueHandler.setProperties(config);
        return queueHandler;
    }
    
    /**
     * 初始化线程池
     */
    private static ThreadPoolExecutor initThreadPool() {
        // 合理配置线程池参数：核心线程数=CPU核心数*2，最大线程数=CPU核心数*4，队列大小=1000
        int corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        int maxPoolSize = Runtime.getRuntime().availableProcessors() * 4;
        int queueCapacity = 1000;
        
        return new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(queueCapacity),
            new ThreadFactoryBuilder().setNameFormat("plugin_task_%d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy() // 队列满时的拒绝策略
        );
    }
    
    /**
     * 提交监控任务
     */
    private static void submitMonitorTasks(ThreadPoolExecutor threadPoolExecutor, Properties config, AtomicInteger taskCount) {
        // 提交杀死任务监控线程
        threadPoolExecutor.execute(new KillCalculateImpl(null, config));
        
        // 提交状态重置任务
        resetStatus(threadPoolExecutor);
    }
    
    /**
     * 启动任务消费循环
     */
    private static void startTaskConsumptionLoop(ThreadPoolExecutor threadPoolExecutor, 
                                                QueueHandler queueHandler, 
                                                Properties config, 
                                                AtomicInteger taskCount, 
                                                int taskLimit, 
                                                String instanceId) throws InterruptedException {
        String serviceName = config.getProperty("service.name");
        ServerManagerUtil.ServiceInstanceConf serviceInstanceConf = ServerManagerUtil.registerServiceInstance(serviceName);
        serviceInstanceConf.setAtomicInteger(taskCount);
        
        while (true) {
            // 服务注册和心跳报告
            ServerManagerUtil.heartbeatReport(serviceInstanceConf);
            ServerManagerUtil.reportTaskNum(serviceInstanceConf);
            ServerManagerUtil.checkServiceRunningMode(serviceInstanceConf);
            ServerManagerUtil.checkServiceSlot(serviceInstanceConf);
            
            // 检查任务数量限制
            if (taskCount.get() > taskLimit) {
                Thread.sleep(1000);
                continue;
            }
            
            // 检查服务停止标志
            Object stopFlag = JedisPoolUtil.redisClient().get(Const.ZDH_PLUGIN_STOP_FLAG_KEY);
            if (stopFlag != null && Boolean.parseBoolean(stopFlag.toString())) {
                if (taskCount.get() == 0) {
                    logger.info("服务停止标志已设置，且无运行中任务，准备关闭服务...");
                    break;
                }
                Thread.sleep(1000);
                continue;
            }
            
            // 处理任务
            handleTask(queueHandler, threadPoolExecutor, taskCount, instanceId);
            
            Thread.sleep(1000);
        }
        
        // 关闭资源
        shutdownResources(threadPoolExecutor);
    }
    
    /**
     * 处理单个任务
     */
    private static void handleTask(QueueHandler queueHandler, 
                                  ThreadPoolExecutor threadPoolExecutor, 
                                  AtomicInteger taskCount, 
                                  String instanceId) {
        Map<String, Object> taskData = queueHandler.handler();
        if (taskData == null) {
            logger.debug("没有新任务");
            return;
        }
        
        logger.info("收到新任务: task_name={}, group_id={}, task={}",
                taskData.getOrDefault("strategy_context", "空"),
                taskData.getOrDefault("group_id", "空"),
                JsonUtil.formatJsonString(taskData));
        
        // 任务预处理和状态检查
        if (!preprocessTask(taskData, instanceId)) {
            return;
        }
        
        // 创建任务并提交到线程池
        Runnable taskRunnable = createTaskRunnable(taskData, taskCount);
        if (taskRunnable != null) {
            Future<?> future = threadPoolExecutor.submit(taskRunnable);
            tasks.put(taskData.get("id").toString(), future);
        }
    }
    
    /**
     * 任务预处理和状态检查
     */
    private static boolean preprocessTask(Map<String, Object> taskData, String instanceId) {
        StrategyInstanceServiceImpl strategyInstanceService = new StrategyInstanceServiceImpl();
        String taskId = taskData.get("id").toString();
        
        // 使用锁防止重复执行
        RLock lock = JedisPoolUtil.redisClient().rLock(taskId);
        if (!lock.tryLock()) {
            logger.info("任务 {} 获取锁失败，跳过执行", taskId);
            return false;
        }
        
        try {
            // 查询任务实例
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(new String[]{taskId});
            if (strategyInstances == null || strategyInstances.isEmpty()) {
                logger.warn("任务 {} 不存在", taskId);
                return false;
            }
            
            StrategyInstance strategyInstance = strategyInstances.get(0);
            
            // 检查任务状态
            if (!strategyInstance.getStatus().equalsIgnoreCase(Const.STATUS_CHECK_DEP_FINISH)) {
                logger.debug("任务 {} 状态不是 {}，跳过执行", taskId, Const.STATUS_CHECK_DEP_FINISH);
                return false;
            }
            
            // 更新任务状态为执行中
            StrategyInstance updateInstance = new StrategyInstance();
            updateInstance.setId(taskId);
            Map<String, Object> runJsMindData = JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());
            runJsMindData.put("instance_id", instanceId);
            updateInstance.setRun_jsmind_data(JsonUtil.formatJsonString(runJsMindData));
            updateInstance.setStatus(Const.STATUS_ETL);
            updateInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
            
            int updateResult = strategyInstanceService.updateStatusAndUpdateTimeByIdAndOldStatus(
                    updateInstance, Const.STATUS_CHECK_DEP_FINISH);
            
            if (updateResult == 0) {
                logger.info("任务 {} 状态已被其他实例更新，跳过执行", taskId);
                return false;
            }
            
            // 更新任务数据
            taskData.put("run_jsmind_data", updateInstance.getRun_jsmind_data());
            return true;
            
        } catch (Exception e) {
            logger.error("任务 {} 预处理失败: ", taskId, e);
            return false;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 创建任务执行线程
     */
    private static Runnable createTaskRunnable(Map<String, Object> taskData, AtomicInteger taskCount) {
        String instanceType = taskData.get("instance_type").toString();
        Properties config = ConfigUtil.getConfig();
        
        try {
            // 使用 InstanceType 枚举优化任务类型判断
            InstanceType type = InstanceType.valueOf(instanceType.toUpperCase());
            
            switch (type) {
                case FILTER:
                    return new FilterCalculateImpl(taskData, taskCount, config);
                case SHUNT:
                    return new ShuntCalculateImpl(taskData, taskCount, config);
                case TOUCH:
                    return new TouchCalculateImpl(taskData, taskCount, config);
                case PLUGIN:
                    return new PluginCalculateImpl(taskData, taskCount, config);
                case ID_MAPPING:
                    return new IdMappingCalculateImpl(taskData, taskCount, config);
                case MANUAL_CONFIRM:
                    return new ManualConfirmCalculateImpl(taskData, taskCount, config);
                case RIGHTS:
                    return new RightsCalculateImpl(taskData, taskCount, config);
                case CODE_BLOCK:
                    return new CodeBlockCalculateImpl(taskData, taskCount, config);
                case TN:
                    return new TnCalculateImpl(taskData, taskCount, config);
                case FUNCTION:
                    return new FunctionCalculateImpl(taskData, taskCount, config);
                case VARPOOL:
                    return new VarPoolCalculateImpl(taskData, taskCount, config);
                case VARIABLE:
                    return new VariableCalculateImpl(taskData, taskCount, config);
                default:
                    LogUtil.error(taskData.get("strategy_id").toString(), taskData.get("id").toString(), 
                            "不支持的任务类型: " + instanceType);
                    setStatus(taskData.get("id").toString(), Const.STATUS_ERROR);
                    return null;
            }
        } catch (IllegalArgumentException e) {
            LogUtil.error(taskData.get("strategy_id").toString(), taskData.get("id").toString(), 
                    "无效的任务类型: " + instanceType);
            setStatus(taskData.get("id").toString(), Const.STATUS_ERROR);
            return null;
        }
    }
    
    /**
     * 关闭资源
     */
    private static void shutdownResources(ThreadPoolExecutor threadPoolExecutor) {
        logger.info("开始关闭资源...");
        
        // 关闭线程池
        threadPoolExecutor.shutdown();
        try {
            if (!threadPoolExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPoolExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPoolExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 关闭Redis连接
        JedisPoolUtil.close();
        
        logger.info("资源关闭完成");
    }
    
    // 以下为原有方法，仅做了少量优化
    
    public static void initLogType(Properties config) {
        String logType = config.getProperty(ConfigUtil.LOG_TYPE, Const.LOG_TYPE_MYSQL);
        LogUtil.logType = logType;
        
        if (logType.equalsIgnoreCase(Const.LOG_TYPE_MONGODB)) {
            String mongodbUrl = config.getProperty(ConfigUtil.LOG_MONGODB_URL, "mongodb://localhost:27017");
            String mongodbDb = config.getProperty(ConfigUtil.LOG_MONGODB_DB, "zdh");
            int maxPoolSize = Integer.parseInt(config.getProperty(ConfigUtil.LOG_MONGODB_MAX_POOL_SIZE, "1"));
            int minPoolSize = Integer.parseInt(config.getProperty(ConfigUtil.LOG_MONGODB_MIN_POOL_SIZE, "1"));
            int maxWaitTime = Integer.parseInt(config.getProperty(ConfigUtil.LOG_MONGODB_MAX_WAIT_TIME, "5"));
            LogUtil.initMongoDb(mongodbUrl, mongodbDb, maxPoolSize, minPoolSize, maxWaitTime);
        }
    }
    
    public static void checkConfig(Properties config) throws Exception {
        if (config.getProperty(ConfigUtil.SERVICE_NAME) == null) {
            throw new Exception("配置信息缺失service.name参数");
        }
        
        if (config.getProperty(ConfigUtil.FILE_PATH) == null || config.getProperty(ConfigUtil.FILE_ROCKSDB_PATH) == null) {
            throw new Exception("配置信息缺失file.path, file.rocksdb.path参数");
        }
        
        int slotNum = Integer.parseInt(config.getProperty(ConfigUtil.TASK_SLOT_TOTAL_NUM, "0"));
        String slot = config.getProperty(ConfigUtil.TASK_SLOT, "0");
        logger.info("服务总槽位: {}, 服务分配槽位: {}", slotNum, slot);
        
        if (slotNum == 0) {
            throw new Exception("服务总槽位配置异常, example: 100");
        }
        
        if (!slot.contains(",") || slot.split(",").length != 2) {
            throw new Exception("任务分配槽位信息配置格式异常, example: 0,99");
        }
        
        if (!NumberUtil.isInteger(slot.split(",")[0]) || !NumberUtil.isInteger(slot.split(",")[1])) {
            throw new Exception("任务分配槽位信息配置只可填写数字");
        }
    }
    
    /**
     * id mapping 初始化redis引擎
     * @param config
     */
    public static void loadIdMappingConf(Properties config) {
        // 获取redis引擎的配置
        Map<String, RedisIdMappingEngineImpl.RedisConf> mappingCodeConf = new HashMap<>();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("id_mapping_code.")) {
                String[] columns = key.split("\\.");
                String idMappingCode = columns[1];
                String engine = columns[2];
                String param = columns[3];
                
                RedisIdMappingEngineImpl.RedisConf redisConf = mappingCodeConf.computeIfAbsent(
                        idMappingCode, k -> new RedisIdMappingEngineImpl.RedisConf());
                
                if ("redis".equalsIgnoreCase(engine)) {
                    String value = config.getProperty(key);
                    switch (param.toLowerCase()) {
                        case "mode":
                            redisConf.setMode(value);
                            break;
                        case "url":
                            redisConf.setUrl(value);
                            break;
                        case "password":
                            redisConf.setPasswd(value);
                            break;
                    }
                }
            }
        }
        RedisIdMappingEngineImpl.redisConfMap = mappingCodeConf;
    }
    
    /**
     * filter 初始化redis引擎
     * @param config
     */
    public static void loadFilterConf(Properties config) {
        // 获取redis引擎的配置
        Map<String, RedisFilterEngineImpl.RedisConf> filterCodeConf = new HashMap<>();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("filter.")) {
                String[] columns = key.split("\\.");
                String filterCode = columns[1];
                String engine = columns[2];
                String param = columns[3];
                
                RedisFilterEngineImpl.RedisConf redisConf = filterCodeConf.computeIfAbsent(
                        filterCode, k -> new RedisFilterEngineImpl.RedisConf());
                
                if ("redis".equalsIgnoreCase(engine)) {
                    String value = config.getProperty(key);
                    switch (param.toLowerCase()) {
                        case "mode":
                            redisConf.setMode(value);
                            break;
                        case "url":
                            redisConf.setUrl(value);
                            break;
                        case "password":
                            redisConf.setPasswd(value);
                            break;
                    }
                }
            }
        }
        RedisFilterEngineImpl.redisConfMap = filterCodeConf;
    }
    
    public static void resetStatus(ThreadPoolExecutor threadPoolExecutor) {
        StrategyInstanceServiceImpl strategyInstanceService = new StrategyInstanceServiceImpl();
        
        threadPoolExecutor.submit(() -> {
            while (true) {
                try {
                    String slotStr = ServerManagerUtil.getReportSlot("");
                    String[] slots = slotStr.split(",");
                    int startSlot = Integer.parseInt(slots[0]);
                    int endSlot = Integer.parseInt(slots[1]);
                    
                    List<StrategyInstance> strategyInstances = strategyInstanceService.selectByStatus(
                            new String[]{Const.STATUS_ETL}, DbQueueHandler.instanceTypes);
                    
                    for (StrategyInstance strategyInstance : strategyInstances) {
                        resetStaleTask(strategyInstance, startSlot, endSlot);
                    }
                    
                    Thread.sleep(60000); // 每分钟执行一次
                } catch (Exception e) {
                    logger.error("重置任务状态失败: ", e);
                    try {
                        Thread.sleep(10000); // 出错后暂停10秒再重试
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
    }
    
    /**
     * 重置过期任务状态
     */
    private static void resetStaleTask(StrategyInstance strategyInstance, int startSlot, int endSlot) {
        Map<String, Object> runJsMindData = JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());
        
        // 跳过异步任务
        boolean isAsync = Boolean.parseBoolean(runJsMindData.getOrDefault(Const.STRATEGY_INSTANCE_IS_ASYNC, "false").toString());
        boolean hasAsyncTaskId = runJsMindData.containsKey(Const.STRATEGY_INSTANCE_ASYNC_TASK_ID);
        
        if (isAsync && !hasAsyncTaskId) {
            return;
        }
        
        // 检查实例ID是否存在
        if (!runJsMindData.containsKey(Const.STRATEGY_INSTANCE_INSTANCE_ID)) {
            return;
        }

        String instanceId = runJsMindData.get(Const.STRATEGY_INSTANCE_INSTANCE_ID).toString();
        // 如果实例还存在，跳过重置
        if (ServerManagerUtil.checkInstanceId(instanceId)) {
            return;
        }
        
        // 检查任务是否属于当前槽位
        long strategyId = Long.parseLong(strategyInstance.getStrategy_id());
        if (strategyId % DEFAULT_SLOT_NUM + 1 >= startSlot && strategyId % DEFAULT_SLOT_NUM + 1 <= endSlot) {
            // 重置任务状态
            RLock resetLock = JedisPoolUtil.redisClient().rLock("reset_task_" + strategyInstance.getId());
            if (resetLock.tryLock()) {
                try {
                    runJsMindData.remove(Const.STRATEGY_INSTANCE_INSTANCE_ID);
                    StrategyInstance updateInstance = new StrategyInstance();
                    updateInstance.setId(strategyInstance.getId());
                    updateInstance.setRun_jsmind_data(JsonUtil.formatJsonString(runJsMindData));
                    updateInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
                    updateInstance.setStatus(Const.STATUS_CHECK_DEP_FINISH);
                    
                    StrategyInstanceServiceImpl strategyInstanceService = new StrategyInstanceServiceImpl();
                    strategyInstanceService.updateStatusAndUpdateTimeByIdAndOldStatus(updateInstance, Const.STATUS_ETL);
                    
                    logger.info("重置过期任务状态: taskId={}, oldInstanceId={}", 
                            strategyInstance.getId(), instanceId);
                } catch (Exception e) {
                    logger.error("重置任务状态失败: taskId={}, error={}", 
                            strategyInstance.getId(), e.getMessage(), e);
                } finally {
                    resetLock.unlock();
                }
            }
        }
    }
    
    public static void setStatus(String taskId, String status) {
        StrategyInstanceServiceImpl strategyInstanceService = new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance = new StrategyInstance();
        strategyInstance.setId(taskId);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
        strategyInstanceService.updateStatusAndUpdateTimeById(strategyInstance);
    }
}