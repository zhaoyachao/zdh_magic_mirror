package com.zyc.magic_mirror.common.redis;

import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

import java.util.Properties;

public class JedisPoolUtil {


    public static RedisClient redisClient;

    public static void connect(Properties properties){

        if(properties.getProperty("redis.mode", "single").equalsIgnoreCase("cluster")){
            cluster(properties);
        }else{
            single(properties);
        }
    }

    public static void close(){
        redisClient.close();
    }

    public static void single(Properties properties){
        String host = properties.getProperty("redis.host");
        int port = Integer.valueOf(properties.getProperty("redis.port"));
        String auth = properties.getProperty("redis.password");
        if(StringUtils.isEmpty(auth)){
            auth = null;
        }
        int poolTimeOut = 0;
        Config config = new Config();
        config.useSingleServer().
                setRetryAttempts(10).
                setRetryInterval(500).
                setAddress("redis://"+host+":"+port).
                setPassword(auth);


        config.setCodec(new StringCodec());
        RedissonClient redissonClient = Redisson.create(config);
        redisClient = new RedisClientImpl(redissonClient);
    }

    public static void cluster(Properties properties){
        Config config = new Config();
        String[] hosts = properties.getProperty("redis.host").split(",");
        String auth = properties.getProperty("redis.password");
        if(StringUtils.isEmpty(auth)){
            auth = null;
        }
        //redis 集群模式
        ClusterServersConfig clusterServersConfig = config.useClusterServers();
        for(String hp:hosts){
            clusterServersConfig.addNodeAddress("redis://"+hp.split(":")[0]+":"+hp.split(":")[1]);
        }
        clusterServersConfig.setPassword(auth);
        clusterServersConfig.setScanInterval(5000);
        clusterServersConfig.setRetryInterval(500);
        clusterServersConfig.setRetryAttempts(10);
        config.setCodec(new StringCodec());
        RedissonClient redissonClient = Redisson.create(config);
        redisClient = new RedisClientImpl(redissonClient);
    }


    public static RedisClient redisClient(){
        if(redisClient != null){
            return redisClient;
        }
        return null;
    }

}
