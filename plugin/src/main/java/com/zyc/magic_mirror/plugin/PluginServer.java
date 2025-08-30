package com.zyc.magic_mirror.plugin;

import cn.hutool.core.util.NumberUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zyc.magic_mirror.common.entity.InstanceType;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.queue.QueueHandler;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.common.util.ServerManagerUtil;
import com.zyc.magic_mirror.plugin.calculate.impl.*;
import com.zyc.magic_mirror.plugin.impl.StrategyInstanceServiceImpl;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 标签处理主类
 */
public class PluginServer {

    private static Logger logger= LoggerFactory.getLogger(PluginServer.class);

    public static Map<String, Future> tasks = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        logger.info("初始化项目");

        try {
            Properties config = new Properties();
            String conf_path = PluginServer.class.getClassLoader().getResource("application.properties").getPath();

            config.load(PluginServer.class.getClassLoader().getResourceAsStream("application.properties"));

            File confFile = new File("conf/application.properties");
            if(confFile.exists()){
                conf_path = confFile.getPath();
                config.load(new FileInputStream(confFile));
            }
            logger.info("加载配置文件路径:{}", conf_path);

            if(config==null){
                throw new Exception("配置信息为空异常");
            }

            logger.info(config.toString());

            initLogType(config);

            checkConfig(config);

            JedisPoolUtil.connect(config);

            AtomicInteger atomicInteger=new AtomicInteger(0);

            String serviceName = config.getProperty("service.name");
            ServerManagerUtil.registerServiceName(serviceName);
            ServerManagerUtil.ServiceInstanceConf serviceInstanceConf = ServerManagerUtil.registerServiceInstance(serviceName);
            serviceInstanceConf.setAtomicInteger(atomicInteger);

            String slot_num = config.getProperty("task.slot.total.num", "0");
            String slot = config.getProperty("task.slot", "-1,-1");
            String instanceId = ServerManagerUtil.buildServiceInstance();
            ServerManagerUtil.reportSlot(instanceId, slot_num, slot);

            loadIdMappingConf(config);

            loadFilterConf(config);

            if(config.get("file.path") == null || config.get("file.rocksdb.path") == null){
                throw new Exception("配置信息缺失file.path, file.rocksdb.path参数");
            }

            int limit = Integer.valueOf(config.getProperty("task.max.num", "50"));

            QueueHandler queueHandler=new DbQueueHandler();
            queueHandler.setProperties(config);

            ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(10, 1024, 20, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("plugin_%d").build());
            //提交一个监控杀死任务线程
            threadPoolExecutor.execute(new KillCalculateImpl(null, config));

            resetStatus(threadPoolExecutor);

            while (true){
                ServerManagerUtil.registerServiceInstance(serviceName);
                ServerManagerUtil.heartbeatReport(serviceInstanceConf);
                ServerManagerUtil.reportTaskNum(serviceInstanceConf);
                ServerManagerUtil.checkServiceRunningMode(serviceInstanceConf);
                ServerManagerUtil.checkServiceSlot(serviceInstanceConf);

                if(atomicInteger.get()>limit){
                    Thread.sleep(1000);
                    continue;
                }
                Object flag_stop = JedisPoolUtil.redisClient().get(Const.ZDH_PLUGIN_STOP_FLAG_KEY);
                if(flag_stop != null && flag_stop.toString().equalsIgnoreCase("true")){
                    if(atomicInteger.get()==0){
                        break;
                    }
                    continue;
                }

                Map m = queueHandler.handler();
                if(m != null){
                    logger.info("task_name: "+m.getOrDefault("strategy_context", "空")+", group_id: "+m.getOrDefault("group_id","空")+", task : "+ JsonUtil.formatJsonString(m));
                    StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
                    //加锁防重执行
                    RLock rLock = JedisPoolUtil.redisClient().rLock(m.get("id").toString());

                    if(!rLock.tryLock()){
                        logger.info("task: {} ,try lock error: ", m.getOrDefault("id", ""));
                        continue;
                    }
                    try{
                        List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(new String[]{m.get("id").toString()});
                        if(strategyInstances==null || strategyInstances.size()<1){
                            continue;
                        }

                        //检查状态
                        if(!strategyInstances.get(0).getStatus().equalsIgnoreCase(Const.STATUS_CHECK_DEP_FINISH)){
                            continue;
                        }
                        //更新状态为执行中
                        StrategyInstance strategyInstance=new StrategyInstance();
                        strategyInstance.setId(m.get("id").toString());
                        Map<String, Object> run_jsmind_data = JsonUtil.toJavaMap(strategyInstances.get(0).getRun_jsmind_data());
                        run_jsmind_data.put("instance_id", instanceId);
                        strategyInstance.setRun_jsmind_data(JsonUtil.formatJsonString(run_jsmind_data));
                        strategyInstance.setStatus(Const.STATUS_ETL);
                        strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
                        strategyInstanceService.updateStatusAndUpdateTimeByIdAndOldStatus(strategyInstance, Const.STATUS_CHECK_DEP_FINISH);

                        //此处不可忽略
                        m.put("run_jsmind_data", strategyInstance.getRun_jsmind_data());
                    }catch (Exception e){
                        logger.error("plugin server check error: ", e);
                    }finally {
                        rLock.unlock();
                    }

                    Runnable runnable=null;
                    String instanceType = m.get("instance_type").toString();
                    if(instanceType.equalsIgnoreCase(InstanceType.FILTER.getCode())){
                        runnable=new FilterCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.SHUNT.getCode())){
                        runnable=new ShuntCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.TOUCH.getCode())){
                        runnable=new TouchCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.PLUGIN.getCode())){
                        runnable=new PluginCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.ID_MAPPING.getCode())){
                        runnable=new IdMappingCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.MANUAL_CONFIRM.getCode())){
                        runnable=new ManualConfirmCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.RIGHTS.getCode())){
                        runnable=new RightsCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.CODE_BLOCK.getCode())){
                        runnable=new CodeBlockCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.TN.getCode())){
                        runnable=new TnCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.FUNCTION.getCode())){
                        runnable=new FunctionCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.VARPOOL.getCode())){
                        runnable=new VarPoolCalculateImpl(m, atomicInteger, config);
                    }else if(instanceType.equalsIgnoreCase(InstanceType.VARIABLE.getCode())){
                        runnable=new VariableCalculateImpl(m, atomicInteger, config);
                    }else{
                        //不支持的任务类型
                        LogUtil.error(m.get("strategy_id").toString(), m.get("id").toString(), "不支持的任务类型, "+instanceType);
                        setStatus(m.get("id").toString(), Const.STATUS_ERROR);
                        continue;
                    }

                    if(runnable != null){
                        Future future = threadPoolExecutor.submit(runnable);
                        PluginServer.tasks.put(m.get("id").toString(), future);
                    }else{
                        logger.error("not found task impl: {}", JsonUtil.formatJsonString(m));
                    }

                }else{
                    logger.debug("not found label task");
                }

                Thread.sleep(1000);
            }
            threadPoolExecutor.shutdownNow();
            JedisPoolUtil.close();
        }catch (Exception e){
            logger.error("plugin server error: ", e);
        }
    }

    public static void initLogType(Properties config){
        String logType = config.getProperty("log.type", Const.LOG_TYPE_MYSQL);
        LogUtil.logType = logType;

        if(logType.equalsIgnoreCase(Const.LOG_TYPE_MONGODB)){
            String mongodbUrl = config.getProperty("log.mongodb.url", "mongodb://localhost:27017");
            String mongodbDb = config.getProperty("log.mongodb.db", "zdh");
            Integer maxPoolSize = Integer.valueOf(config.getProperty("log.mongodb.maxPoolSize", "1"));
            Integer minPoolSize = Integer.valueOf(config.getProperty("log.mongodb.minPoolSize", "1"));
            Integer maxWaitTime = Integer.valueOf(config.getProperty("log.mongodb.maxWaitTime", "5"));
            LogUtil.initMongoDb(mongodbUrl, mongodbDb, maxPoolSize, minPoolSize, maxWaitTime);
        }
    }

    public static void checkConfig(Properties config) throws Exception {

        if(config.get("service.name") == null){
            throw new Exception("配置信息缺失service.name参数");
        }

        if(config.get("file.path") == null || config.get("file.rocksdb.path") == null){
            throw new Exception("配置信息缺失file.path, file.rocksdb.path参数");
        }

        int slot_num = Integer.valueOf(config.getProperty("task.slot.total.num", "0"));
        String slot = config.getProperty("task.slot", "0");
        logger.info("服务总槽位: "+slot_num+", 服务分配槽位: "+slot);

        if(slot_num==0){
            throw new Exception("服务总槽位配置异常,example: 100");
        }

        if(!slot.contains(",") || slot.split(",").length != 2){
            throw new Exception("任务分配槽位信息配置格式异常,example: 0,99");
        }

        if(!NumberUtil.isInteger(slot.split(",")[0])){
            throw new Exception("任务分配槽位信息配置只可填写数字");
        }
        if(!NumberUtil.isInteger(slot.split(",")[1])){
            throw new Exception("任务分配槽位信息配置只可填写数字");
        }

    }

    /**
     * id mapping 初始化redis引擎
     * @param config
     */
    public static void loadIdMappingConf(Properties config){
        //获取redis引擎的配置
        Map<String, RedisIdMappingEngineImpl.RedisConf> mapping_code_conf = new HashMap<>();
        for(String key: config.stringPropertyNames()){
            if(key.startsWith("id_mapping_code.")){
                String[] columns = key.split("\\.");
                String id_mapping_code = columns[1];
                String engine = columns[2];
                String param = columns[3];
                RedisIdMappingEngineImpl.RedisConf redisConf =new RedisIdMappingEngineImpl.RedisConf();
                if(mapping_code_conf.containsKey(id_mapping_code)){
                    redisConf = mapping_code_conf.get(id_mapping_code);
                }else{
                    mapping_code_conf.put(id_mapping_code, redisConf);
                }
                if(engine.equalsIgnoreCase("redis")){
                  if(param.equalsIgnoreCase("mode")){
                      redisConf.setMode(config.getProperty(key));
                  }else if(param.equalsIgnoreCase("url")){
                      redisConf.setUrl(config.getProperty(key));
                  }else if(param.equalsIgnoreCase("password")){
                      redisConf.setPasswd(config.getProperty(key));
                  }
                }

            }
        }
        RedisIdMappingEngineImpl.redisConfMap = mapping_code_conf;
    }

    /**
     * filter 初始化redis引擎
     * @param config
     */
    public static void loadFilterConf(Properties config){
        //获取redis引擎的配置
        Map<String, RedisFilterEngineImpl.RedisConf> mapping_code_conf = new HashMap<>();
        for(String key: config.stringPropertyNames()){
            if(key.startsWith("filter.")){
                String[] columns = key.split("\\.");
                String id_mapping_code = columns[1];
                String engine = columns[2];
                String param = columns[3];
                RedisFilterEngineImpl.RedisConf redisConf =new RedisFilterEngineImpl.RedisConf();
                if(mapping_code_conf.containsKey(id_mapping_code)){
                    redisConf = mapping_code_conf.get(id_mapping_code);
                }else{
                    mapping_code_conf.put(id_mapping_code, redisConf);
                }
                if(engine.equalsIgnoreCase("redis")){
                    if(param.equalsIgnoreCase("mode")){
                        redisConf.setMode(config.getProperty(key));
                    }else if(param.equalsIgnoreCase("url")){
                        redisConf.setUrl(config.getProperty(key));
                    }else if(param.equalsIgnoreCase("password")){
                        redisConf.setPasswd(config.getProperty(key));
                    }
                }

            }
        }
        RedisFilterEngineImpl.redisConfMap = mapping_code_conf;
    }

    public static void resetStatus(ThreadPoolExecutor threadPoolExecutor){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                while (true){
                    try{
                        String slotStr = ServerManagerUtil.getReportSlot("");
                        String[] slots = slotStr.split(",");
                        int slot_num = 100;
                        int start_slot =  Integer.valueOf(slots[0]);
                        int end_slot =  Integer.valueOf(slots[1]);
                        //查询复合slot的策略
                        List<StrategyInstance> strategyInstances = strategyInstanceService.selectByStatus(new String[]{Const.STATUS_ETL}, DbQueueHandler.instanceTypes);

                        for (StrategyInstance strategyInstance: strategyInstances){
                            Map run_jsmind_data = JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());
                            if(run_jsmind_data.containsKey(Const.STRATEGY_INSTANCE_IS_ASYNC) && run_jsmind_data.getOrDefault(Const.STRATEGY_INSTANCE_IS_ASYNC, "false").toString().equalsIgnoreCase("true")
                                    && !run_jsmind_data.containsKey(Const.STRATEGY_INSTANCE_ASYNC_TASK_ID)){
                                continue;
                            }
                            if(run_jsmind_data.containsKey("instance_id")){
                                //当前实例还存在,则跳过
                                if(ServerManagerUtil.checkInstanceId(run_jsmind_data.get("instance_id").toString())){
                                    continue;
                                }
                            }else{
                                continue;
                            }
                            if(Long.valueOf(strategyInstance.getStrategy_id())% slot_num + 1 >= start_slot && Long.valueOf(strategyInstance.getStrategy_id())%slot_num + 1 <= end_slot){
                                run_jsmind_data.remove("instance_id");
                                strategyInstance.setRun_jsmind_data(JsonUtil.formatJsonString(run_jsmind_data));
                                strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
                                strategyInstance.setStatus(Const.STATUS_CHECK_DEP_FINISH);
                                RLock rLock = JedisPoolUtil.redisClient().rLock("reset_task_"+strategyInstance.getId());
                                try{
                                    if(!rLock.tryLock()){
                                        logger.info("reset_task: {} ,try lock error: ", strategyInstance.getId());
                                        continue;
                                    }
                                    strategyInstanceService.updateStatusAndUpdateTimeByIdAndOldStatus(strategyInstance, Const.STATUS_ETL);
                                }catch (Exception e){
                                    e.printStackTrace();
                                }finally {
                                    rLock.unlock();
                                }

                            }
                        }
                        Thread.sleep(1000*60);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }

            }
        });
    }
    public static void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
        strategyInstanceService.updateStatusAndUpdateTimeById(strategyInstance);
    }

}
