package com.zyc.magic_mirror.common.util;

import cn.hutool.system.SystemUtil;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务管理工具类
 * 服务注册
 */
public class ServerManagerUtil {

    private static Logger logger = LoggerFactory.getLogger(ServerManagerUtil.class);
    public static String SERVICE_NAME_KEY = "ZDH_SERVER_MANAGER_SERVICE_NAME";

    //服务实例主KEY
    public static String SERVICE_INSTANCE_KEY = "ZDH_SERVER_MANAGER_SERVICE_NAME:";

    public static String SERVICE_MODE_STOP = "stop";
    public static String SERVICE_MODE_SUSPEND = "suspend";
    public static String SERVICE_MODE_RUN = "run";


    /**
     * 注册服务名称
     * @param serviceName
     */
    public static void registerServiceName(String serviceName){
        JedisPoolUtil.redisClient().hSet(SERVICE_NAME_KEY, serviceName, getServiceNameKey(serviceName));
    }

    /**
     * 获取服务实例
     * @return
     */
    public static String buildServiceInstance(){
        String address = SystemUtil.getHostInfo().getName();
        String pid = SystemUtil.getCurrentPID()+"";
        return address+"_"+pid;
    }

    /**
     * 注册服务实例
     * @param serviceName 服务名称
     * @return ServiceInstanceConf 服务注册信息
     */
    public static ServiceInstanceConf registerServiceInstance(String serviceName){
        ServiceInstanceConf serviceInstanceConf = ServiceInstanceConf.quick(serviceName);
        String instance = getServiceInstanceKey(serviceInstanceConf);
        String key = getServiceNameKey(serviceName);
        JedisPoolUtil.redisClient().hSet(key, instance, "0");
        //默认5分钟
        //JedisPoolUtil.redisClient().expire(key, 60*5L);
        return serviceInstanceConf;
    }

    public static String getServiceNameKey(String serviceName){
        return SERVICE_INSTANCE_KEY+serviceName;
    }

    public static String getServiceInstanceKey(ServiceInstanceConf serviceInstanceConf){
        return serviceInstanceConf.getHost()+"_"+serviceInstanceConf.getPid();
    }

    /**
     * 心跳上报
     * @param serviceInstanceConf
     */
    public static void heartbeatReport(ServiceInstanceConf serviceInstanceConf){
        String key = getServiceNameKey(serviceInstanceConf.getService_name());
        String instance = getServiceInstanceKey(serviceInstanceConf);
        serviceInstanceConf.setLast_time(System.currentTimeMillis()+"");
        String value = JsonUtil.formatJsonString(serviceInstanceConf);
        JedisPoolUtil.redisClient().hSet(instance, "service", value);
        JedisPoolUtil.redisClient().expire(instance, 60*1L);

        //检查其他实例是否可用
        Map<Object, Object> objectObjectMap = JedisPoolUtil.redisClient().hGetAll(key);

        if(objectObjectMap == null){
            return ;
        }

        for(Map.Entry<Object, Object> entrie:objectObjectMap.entrySet()){
            String instance1 = entrie.getKey().toString();
            //检查实例是否存在,不存在则实例进入待删除队列
            if(!JedisPoolUtil.redisClient().isExists(instance1)){
                JedisPoolUtil.redisClient().hDel(key, instance1);
            }
        }

    }

    /**
     * 检查服务执行模式
     * 当前仅支持stop, suspend 这2种模式
     * stop 模式：直接停止当前服务
     * suspend 模式：暂停当前服务,等待服务恢复
     *
     * @param serviceInstanceConf
     * @return 返回
     */
    public static String checkServiceRunningMode(ServiceInstanceConf serviceInstanceConf){
        String instance = getServiceInstanceKey(serviceInstanceConf);

        //获取可执行模式
        Object mode = JedisPoolUtil.redisClient().hGet(instance, "mode");
        if(mode!=null){
            if(mode.toString().equalsIgnoreCase(SERVICE_MODE_STOP)){
                //停止
                while (true){
                    mode = JedisPoolUtil.redisClient().hGet(instance, "mode");
                    if(!mode.toString().equalsIgnoreCase(SERVICE_MODE_STOP)){
                        break;
                    }
                    Object taskNum = JedisPoolUtil.redisClient().hGet(instance, "task_num");
                    if(taskNum == null || (taskNum != null && Integer.valueOf(taskNum.toString())==0) ){
                        JedisPoolUtil.redisClient().hDel(instance, "mode");
                        logger.info("服务停止...");
                        System.exit(0);
                    }
                    try{
                        Thread.sleep(1000);
                    }catch (Exception e){

                    }

                }
            }else if(mode.toString().equalsIgnoreCase(SERVICE_MODE_SUSPEND)){
                while (true){
                    try {
                        Thread.sleep(1000*10L);
                        heartbeatReport(serviceInstanceConf);
                        reportTaskNum(serviceInstanceConf);
                        Object value1 = JedisPoolUtil.redisClient().hGet(instance, "mode");
                        if(value1 == null || !value1.toString().equalsIgnoreCase("suspend")){
                            logger.info("服务暂停恢复...");
                            break;
                        }

                        logger.info("服务暂停中...");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return checkServiceRunningMode(serviceInstanceConf);
            }
        }else{
            JedisPoolUtil.redisClient().hSet(instance, "mode", SERVICE_MODE_RUN);
            JedisPoolUtil.redisClient().expire(instance, 60*1L);
        }

        return SERVICE_MODE_RUN;
    }


    /**
     * 检查服务槽位,无信息默认填充
     * @param serviceInstanceConf
     */
    public static void checkServiceSlot(ServiceInstanceConf serviceInstanceConf){
        String instance = getServiceInstanceKey(serviceInstanceConf);
        Object slot = JedisPoolUtil.redisClient().hGet(instance, "slot");
        if(slot == null){
            reportSlot(instance, "0", "-1,-1");
        }
    }

    public static void reportSlot(String instaceId, String slot_num, String slot){
        JedisPoolUtil.redisClient().hSet(instaceId, "slot", slot);
        JedisPoolUtil.redisClient().expire(instaceId, 60*1L);
    }

    public static void reportTaskNum(ServiceInstanceConf serviceInstanceConf){
        String instance = getServiceInstanceKey(serviceInstanceConf);
        JedisPoolUtil.redisClient().hSet(instance, "task_num", serviceInstanceConf.atomicInteger.toString());
        JedisPoolUtil.redisClient().expire(instance, 60*1L);
    }


    public static String getReportSlot(String instaceId){
        if(StringUtils.isEmpty(instaceId)){
            instaceId = buildServiceInstance();
        }
        Object slot = JedisPoolUtil.redisClient().hGet(instaceId, "slot");
        if(slot == null){
            return "-1,-1";
        }
        return slot.toString();
    }

    public static boolean checkInstanceId(String instaceId){
        try{
            return JedisPoolUtil.redisClient().isExists(instaceId);
        }catch (Exception e){
            logger.error("检查实例是否存异常: ",e);
            return true;
        }
    }

    public static String getReportVersionTag(String instaceId){
        if(StringUtils.isEmpty(instaceId)){
            instaceId = buildServiceInstance();
        }
        Object versionTag = JedisPoolUtil.redisClient().hGet(instaceId, "version_tag");
        if(versionTag == null){
            return "";
        }
        return versionTag.toString();
    }

    public static class ServiceInstanceConf{

        private String service_name;

        private String register_time;

        private String host;

        private String pid;

        private String last_time;

        private AtomicInteger atomicInteger;

        public String getService_name() {
            return service_name;
        }

        public void setService_name(String service_name) {
            this.service_name = service_name;
        }

        public String getRegister_time() {
            return register_time;
        }

        public void setRegister_time(String register_time) {
            this.register_time = register_time;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getPid() {
            return pid;
        }

        public void setPid(String pid) {
            this.pid = pid;
        }

        public String getLast_time() {
            return last_time;
        }

        public void setLast_time(String last_time) {
            this.last_time = last_time;
        }

        public AtomicInteger getAtomicInteger() {
            return atomicInteger;
        }

        public void setAtomicInteger(AtomicInteger atomicInteger) {
            this.atomicInteger = atomicInteger;
        }

        public static ServiceInstanceConf quick(String service_name){
            ServiceInstanceConf serviceInstanceConf = new ServiceInstanceConf();
            String address = SystemUtil.getHostInfo().getName();
            String pid = SystemUtil.getCurrentPID()+"";

            serviceInstanceConf.setService_name(service_name);
            serviceInstanceConf.setHost(address);
            serviceInstanceConf.setPid(pid);
            serviceInstanceConf.setRegister_time(System.currentTimeMillis()+"");

            return serviceInstanceConf;
        }
    }
}
