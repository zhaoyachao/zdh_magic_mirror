package com.zyc.plugin;

import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.queue.QueueHandler;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.impl.*;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.HashMap;
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
        Map<String,String> params=new HashMap<>();
        if(args!=null && args.length>0){
           for (int i=0;i<args.length;i+=2){
               if(args.length>(i+1)) {
                   params.put(args[i], args[i + 1]);
               }
           }
        }

        try {
            String dir="./conf";
            Properties config = new Properties();
            if(params.containsKey("-conf")){
                dir = params.getOrDefault("-conf", dir);
                config = loadConfig(dir+"/application.properties");
            }else{
                config.load(PluginServer.class.getClassLoader().getResourceAsStream("application.properties"));
            }

            if(config==null){
                throw new Exception("配置信息为空异常");
            }

            logger.info(config.toString());

            checkConfig(config);

            loadIdMappingConf(config);

            loadFilterConf(config);

            JedisPoolUtil.connect(config);

            if(config.get("file.path") == null || config.get("file.rocksdb.path") == null){
                throw new Exception("配置信息缺失file.path, file.rocksdb.path参数");
            }

            int limit = Integer.valueOf(config.getProperty("task.max.num", "50"));

            QueueHandler queueHandler=new DbQueueHandler();
            queueHandler.setProperties(config);

            AtomicInteger atomicInteger=new AtomicInteger(0);
            ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(10, 100, 20, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>());
            //提交一个监控杀死任务线程
            threadPoolExecutor.execute(new KillCalculateImpl(null, config));
            resetStatus();
            while (true){
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
                    logger.info("task : "+JSON.toJSONString(m));
                    StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
                    //加锁防重执行
                    RLock rLock = JedisPoolUtil.redisClient().rLock(m.get("id").toString());

                    if(!rLock.tryLock(1L, 5L, TimeUnit.SECONDS)){
                        logger.info("task: {} ,try lock error: ", m.getOrDefault("id", ""));
                        continue;
                    }

                    //更新状态为执行中
                    StrategyInstance strategyInstance=new StrategyInstance();
                    strategyInstance.setId(m.get("id").toString());
                    strategyInstance.setStatus("etl");
                    strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
                    Runnable runnable=null;
                    if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.FILTER.getCode())){
                        runnable=new FilterCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.SHUNT.getCode())){
                        runnable=new ShuntCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.TOUCH.getCode())){
                        runnable=new TouchCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.PLUGIN.getCode())){
                        runnable=new PluginCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.ID_MAPPING.getCode())){
                        runnable=new IdMappingCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.MANUAL_CONFIRM.getCode())){
                        runnable=new ManualConfirmCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.RIGHTS.getCode())){
                        runnable=new RightsCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.CODE_BLOCK.getCode())){
                        runnable=new CodeBlockCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.TN.getCode())){
                        runnable=new TnCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.FUNCTION.getCode())){
                        runnable=new FunctionCalculateImpl(m, atomicInteger, config);
                    }else{
                        //不支持的任务类型
                        LogUtil.error(m.get("strategy_id").toString(), m.get("id").toString(), "不支持的任务类型, "+m.get("instance_type").toString());
                        setStatus(m.get("id").toString(), "error");
                        continue;
                    }

                    if(runnable != null){
                        Future future = threadPoolExecutor.submit(runnable);
                        PluginServer.tasks.put(m.get("id").toString(), future);
                    }else{
                        logger.error("not found task impl: {}", JSON.toJSONString(m));
                    }

                }else{
                    logger.debug("not found label task");
                }

                Thread.sleep(1000);
            }
            threadPoolExecutor.shutdownNow();
            JedisPoolUtil.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static Properties loadConfig(String path) throws Exception {
        Properties prop =new Properties();
        try{
            File file=new File(path);
            prop.load(new FileInputStream(file));
        }catch (Exception e){
            throw new Exception("加载配置文件异常,",e.getCause());
        }
        return prop;
    }

    public static void checkConfig(Properties config) throws Exception {

        if(config.get("file.path") == null || config.get("file.rocksdb.path") == null){
            throw new Exception("配置信息缺失file.path, file.rocksdb.path参数");
        }

        int slot_num = Integer.valueOf(config.getProperty("task.slot.total.num", "0"));
        String slot = config.getProperty("task.slot", "0");
        logger.info("服务总槽位: "+slot_num+", 服务分配槽位: "+slot);
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
                      redisConf.setMode(param);
                  }else if(param.equalsIgnoreCase("url")){
                      redisConf.setUrl(param);
                  }else if(param.equalsIgnoreCase("passwd")){
                      redisConf.setPasswd(param);
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
                        redisConf.setMode(param);
                    }else if(param.equalsIgnoreCase("url")){
                        redisConf.setUrl(param);
                    }else if(param.equalsIgnoreCase("passwd")){
                        redisConf.setPasswd(param);
                    }
                }

            }
        }
        RedisFilterEngineImpl.redisConfMap = mapping_code_conf;
    }

    public static void resetStatus(){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        strategyInstanceService.updateStatus2CheckFinish(Const.STATUS_ETL, DbQueueHandler.instanceTypes);
    }
    public static void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
    }

}
