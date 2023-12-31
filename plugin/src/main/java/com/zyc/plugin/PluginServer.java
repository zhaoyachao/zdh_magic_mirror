package com.zyc.plugin;

import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.queue.QueueHandler;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.impl.*;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.Date;
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

            JedisPoolUtil.connect(config);

            if(config.get("file.path") == null || config.get("file.rocksdb.path") == null){
                throw new Exception("配置信息缺失file.path, file.rocksdb.path参数");
            }

            int limit = Integer.valueOf(config.getProperty("task.max.num", "50"));

            QueueHandler queueHandler=new DbQueueHandler();

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
                        threadPoolExecutor.execute(runnable);
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

    public static void resetStatus(){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        strategyInstanceService.updateStatus2CheckFinish(Const.STATUS_ETL, DbQueueHandler.instanceTypes);
    }
    public static void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(new Date().getTime()));
        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
    }

}
