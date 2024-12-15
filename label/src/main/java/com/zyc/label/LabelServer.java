package com.zyc.label;

import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.queue.QueueHandler;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.ServerManagerUtil;
import com.zyc.label.calculate.impl.*;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import com.zyc.rqueue.RQueueMode;
import org.apache.commons.lang3.StringUtils;
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
public class LabelServer {

    private static Logger logger= LoggerFactory.getLogger(LabelServer.class);

    public static Map<String, Future> tasks = new ConcurrentHashMap<>();

    public static ExecutorService fixedExecutorService = Executors.newFixedThreadPool(1);
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
                config.load(LabelServer.class.getClassLoader().getResourceAsStream("application.properties"));
            }

            if(config==null){
                throw new Exception("找不到配置文件");
            }

            logger.info(config.toString());

            checkConfig(config);

            JedisPoolUtil.connect(config);

            initRQueue(config);

            String serviceName = config.getProperty("service.name");
            ServerManagerUtil.registerServiceName(serviceName);
            ServerManagerUtil.ServiceInstanceConf serviceInstanceConf = ServerManagerUtil.registerServiceInstance(serviceName);
            String slot_num = config.getProperty("task.slot.total.num", "0");
            String slot = config.getProperty("task.slot", "-1,-1");
            String instanceId = ServerManagerUtil.buildServiceInstance();
            ServerManagerUtil.reportSlot(instanceId, slot_num, slot);

            consumerLabelDoubleCheck();

            int limit = Integer.valueOf(config.getProperty("task.max.num", "50"));

            QueueHandler queueHandler=new DbQueueHandler();
            queueHandler.setProperties(config);
            AtomicInteger atomicInteger=new AtomicInteger(0);
            ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(10, 100, 20, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>());
            //提交一个监控杀死任务线程
            threadPoolExecutor.execute(new KillCalculateImpl(null, config));

            resetStatus(config);

            while (true){
                ServerManagerUtil.heartbeatReport(serviceInstanceConf);
                ServerManagerUtil.checkServiceRunningMode(serviceInstanceConf);

                if(atomicInteger.get()>limit){
                    Thread.sleep(1000);
                    continue;
                }

                Object flag_stop = JedisPoolUtil.redisClient().get(Const.ZDH_LABEL_STOP_FLAG_KEY);
                if(flag_stop != null && flag_stop.toString().equalsIgnoreCase("true")){
                    if(atomicInteger.get()==0){
                        break;
                    }
                    continue;
                }


                Map m = queueHandler.handler();
                if(m != null){
                    logger.info("task_name: "+m.getOrDefault("strategy_context", "空")+", group_id: "+m.getOrDefault("group_id","空")+", task : "+JSON.toJSONString(m));
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
                        if(!strategyInstances.get(0).getStatus().equalsIgnoreCase("check_dep_finish")){
                            continue;
                        }
                        //更新状态为执行中
                        StrategyInstance strategyInstance=new StrategyInstance();
                        strategyInstance.setId(m.get("id").toString());
                        strategyInstance.setStatus("etl");
                        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);

                    }catch (Exception e){
                        logger.error("label server check error: ", e);
                    }finally {
                        rLock.unlock();
                    }
                    Runnable runnable=null;
                    if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.LABEL.getCode())){
                        runnable=new LabelCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.CROWD_OPERATE.getCode())){
                        runnable=new CrowdOperateCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.CROWD_FILE.getCode())){
                        runnable=new CrowdFileCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.CROWD_RULE.getCode())){
                        runnable=new CrowdRuleCalculateImpl(m, atomicInteger, config);
                    }else if(m.get("instance_type").toString().equalsIgnoreCase(InstanceType.CUSTOM_LIST.getCode())){
                        runnable=new CustomListCalculateImpl(m, atomicInteger, config);
                    }else{
                        //不支持的任务类型
                        LogUtil.error(m.get("strategy_id").toString(), m.get("id").toString(), "不支持的任务类型, "+m.get("instance_type").toString());
                        setStatus(m.get("id").toString(), "error");
                        continue;
                    }
                    Future future = threadPoolExecutor.submit(runnable);
                    tasks.put(m.get("id").toString(), future);
                }else{
                    logger.debug("not found label task");
                }

                Thread.sleep(1000);
            }

            threadPoolExecutor.shutdownNow();

            JedisPoolUtil.close();
        }catch (Exception e){
            logger.error("label server error: ", e);
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

    public static void initRQueue(Properties config){
        String host = config.getProperty("redis.host");
        String port = config.getProperty("redis.port");
        String auth = config.getProperty("redis.password");
        if(config.getProperty("redis.mode", "single").equalsIgnoreCase("cluster")){
            RQueueManager.buildDefault(host, auth);
        }else{
            RQueueManager.buildDefault(host+":"+port, auth);
        }
    }

    public static void resetStatus(Properties config){
        String slotStr = ServerManagerUtil.getReportSlot("");
        String[] slots = slotStr.split(",");
        int slot_num = 100;
        int start_slot =  Integer.valueOf(slots[0]);
        int end_slot =  Integer.valueOf(slots[1]);

        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        strategyInstanceService.updateStatus2CheckFinishBySlot(Const.STATUS_ETL, DbQueueHandler.instanceTypes,start_slot,end_slot,slot_num);
    }

    public static void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
    }

    /**
     * 标签任务-依赖检查未完成重置状态
     */
    public static void consumerLabelDoubleCheck(){
        fixedExecutorService.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    while (true){
                        RQueueClient rQueueClient = RQueueManager.getRQueueClient(Const.LABEL_DOUBLE_CHECK_DEPENDS_QUEUE_NAME, RQueueMode.DELAYEDQUEUE);
                        Object o = rQueueClient.poll();
                        if(o != null && !StringUtils.isEmpty(o.toString())){
                            //重置实例状态为
                            setStatus(o.toString(), Const.STATUS_CHECK_DEP_FINISH);
                        }
                    }
                } catch (Exception e) {
                    logger.error("label server consumerLabelDoubleCheck error: ", e);
                }

            }
        });
    }
}
