package com.zyc.common.util;

import cn.hutool.system.SystemUtil;
import com.zyc.common.redis.JedisPoolUtil;

/**
 * 服务管理工具类
 * 服务注册
 */
public class ServerManagerUtil {

    public static String SERVICE_NAME_KEY = "ZDH_SERVER_MANAGER_SERVICE_NAME";

    public static String SERVICE_INSTANCE_KEY = "ZDH_SERVER_MANAGER_SERVICE_NAME:";

    /**
     * 注册服务名称
     * @param serviceName
     */
    public static void registerServiceName(String serviceName){
        JedisPoolUtil.redisClient().hSet(SERVICE_NAME_KEY, serviceName, SERVICE_INSTANCE_KEY+serviceName);
    }

    public static void registerServiceInstance(String serviceName){

        String address = SystemUtil.getHostInfo().getAddress();
        String pid = SystemUtil.getCurrentPID()+"";

        String instance = address+"_"+pid;
        JedisPoolUtil.redisClient().hSet(SERVICE_INSTANCE_KEY+serviceName, instance, "");
    }

}
