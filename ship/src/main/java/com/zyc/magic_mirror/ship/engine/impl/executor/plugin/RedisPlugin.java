package com.zyc.magic_mirror.ship.engine.impl.executor.plugin;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Administrator
 */
public class RedisPlugin implements Plugin{

    private static Logger logger= LoggerFactory.getLogger(RedisPlugin.class);

    private static Cache<String, RedisUtil> cache = CacheBuilder.newBuilder()
            // 键使用弱引用（GC 时可能被回收）
            .weakKeys()
            // 值使用软引用（内存不足时可能被回收）
            .softValues()
            .expireAfterAccess(60, TimeUnit.MINUTES) // 最后一次访问后 60 分钟过期
            .expireAfterWrite(120, TimeUnit.MINUTES) // 最后一次写入后 120 分钟过期（取较早）
            .build();

    private String rule_id;
    private Map<String, Object> run_jsmind_data;
    private StrategyInstance strategyInstance;
    private ShipEvent shipEvent;
    private Map<String, Object> user_param;

    public RedisPlugin(String rule_id, Map<String, Object> run_jsmind_data, StrategyInstance strategyInstance, ShipEvent shipEvent, Map<String, Object> user_param){
        this.rule_id = rule_id;
        this.run_jsmind_data = run_jsmind_data;
        this.strategyInstance = strategyInstance;
        this.shipEvent = shipEvent;
        this.user_param = user_param;
    }

    @Override
    public String getName() {
        return "redis";
    }

    @Override
    public boolean execute() throws ExecutionException, InterruptedException {
        RedisUtil redisUtil=null;
        try{
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson((run_jsmind_data).get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

            Properties props = new Properties();

            for (Map<String,Object> param: rule_params){
                String key = param.get("param_code").toString();
                String value = param.getOrDefault("param_value", "").toString();
                if(!StringUtils.isEmpty(value)){
                    props.put(key, value);
                }
            }

            if (!props.containsKey("url")) {
                throw new Exception("url 参数为空");
            }
            if (!props.containsKey("key")) {
                throw new Exception("key 参数为空");
            }
//            if (!props.containsKey("value")) {
//                throw new Exception("url 参数为空");
//            }
//            if (!props.containsKey("mode")) {
//                throw new Exception("url 参数为空");
//            }
//            if (!props.containsKey("expire")) {
//                throw new Exception("url 参数为空");
//            }
//            if (!props.containsKey("password")) {
//                throw new Exception("url 参数为空");
//            }
            String url = props.getProperty("url","");
            String key = props.getProperty("key", "");
            String value = props.getProperty("value", "");
            String mode = props.getProperty("mode", "");
            Long expire = Long.valueOf(props.getProperty("expire", "-1"));
            String password = props.getProperty("password", "");
            String command = props.getProperty("command", "");


            Jinjava jinjava = new Jinjava();
            url = jinjava.render(url, this.user_param);
            key = jinjava.render(key, this.user_param);
            value = jinjava.render(value, this.user_param);

            redisUtil = cache.getIfPresent(url);
            if(redisUtil == null){
                redisUtil = new RedisUtil(url, password);
                cache.put(url, redisUtil);
            }else{
                if(redisUtil.redissonClient.isShutdown()){
                    redisUtil = new RedisUtil(url, password);
                    cache.put(url, redisUtil);
                }
            }

            RetParam obj = execute(redisUtil, command, key, value, expire, mode);

            if(obj.getCode()==0){
                return true;
            }
        }catch (Exception e){
            logger.error("ship plugin kafka error: ", e);
        }finally {

        }
        return false;
    }

    /**
     * 解析并执行 Redis 命令
     * @param command 命令字符串，格式如 "SET key value"
     * @return 命令执行结果
     */
    private RetParam execute(RedisUtil redisUtil,String command, String key, String value, Long seconds, String nxOrxx) {
        RetParam retParam = new RetParam();
        retParam.setCode(0);
        boolean success;
        Object obj;
        try {
            switch (command) {
                // 键值对操作
                case "set":
                    success = redisUtil.set(key, value, seconds, nxOrxx);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "get":
                    obj = redisUtil.get(key);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                case "del":
                    success = redisUtil.del(key);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "exists":
                    success = redisUtil.exists(key);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "expire":
                    success = redisUtil.expire(key, seconds);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                // 哈希操作
                case "hset":
                    String[] parts = value.split("\\s+", 2);
                    success = redisUtil.hset(key, parts[0] ,parts[1], seconds, nxOrxx);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "hget":
                    obj = redisUtil.hget(key, value);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                case "hgetall":
                    obj = redisUtil.hgetall(key);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                case "hdel":
                    obj = redisUtil.hdel(key, value);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    // 列表操作
                case "lpush":
                    success = redisUtil.lpush(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "rpush":
                    success = redisUtil.rpush(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "lrange":
                    String[] parts2 = value.split("\\s+", 2);
                    obj = redisUtil.lrange(key, Integer.parseInt(parts2[0]) ,Integer.parseInt(parts2[1]));
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                case "lpop":
                    obj = redisUtil.lpop(key);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                case "rpop":
                    obj = redisUtil.rpop(key);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                // 集合操作
                case "sadd":
                    success = redisUtil.sadd(key,value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "smembers":
                    obj = redisUtil.smembers(key);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                case "srem":
                    success = redisUtil.srem(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "sismember":
                    success = redisUtil.sismember(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                // 有序集合操作
                case "zadd":
                    success = redisUtil.zadd(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "zrem":
                    success = redisUtil.zrem(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                case "zmembers":
                    obj = redisUtil.zmembers(key);
                    if(obj != null){
                        retParam.setCode(-1);
                    }
                    retParam.setObj(obj);
                    return retParam;
                case "zismember":
                    success = redisUtil.zismember(key, value);
                    if(!success){
                        retParam.setCode(-1);
                    }
                    return retParam;
                default:
                    obj = "Unsupported command: " + command;
            }
        } catch (Exception e) {
            e.printStackTrace();
            retParam.setCode(-1);
        } finally {
            redisUtil.close();
        }
        return retParam;
    }


    public static class RetParam{
        private int code;

        private Object obj;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public Object getObj() {
            return obj;
        }

        public void setObj(Object obj) {
            this.obj = obj;
        }
    }
    public static class RedisUtil {
        private RedissonClient redissonClient;

        public RedisUtil(String address, String password) {

            if (!address.contains(",")) {
                initSingleRedis(address, password);
            } else if (address.contains(",")) {
                initClusterRedis(address, password);
            } else {
                throw new IllegalArgumentException("Unsupported Redis mode: " );
            }
        }

        private void initSingleRedis(String address, String password) {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            if(StringUtils.isEmpty(password)){
                password = null;
            }
            int poolTimeOut = 0;
            Config config = new Config();
            config.setThreads(1);
            config.useSingleServer().
                    setAddress("redis://"+host+":"+port).
                    setPassword(password);

            config.setCodec(new StringCodec());
            this.redissonClient = Redisson.create(config);
        }

        private void initClusterRedis(String address, String password) {
            String[] nodes = address.split(",");
            Config config = new Config();
            config.setThreads(1);
            ClusterServersConfig clusterServersConfig = config.useClusterServers();
            for (String node : nodes) {
                String[] parts = node.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                clusterServersConfig.addNodeAddress("redis://"+host+":"+port);
            }
            if(StringUtils.isEmpty(password)){
                password = null;
            }

            clusterServersConfig.setPassword(password);
            config.setCodec(new StringCodec());
            this.redissonClient = Redisson.create(config);

        }

        public boolean set(String key, String value, Long seconds, String nxOrxx) {
            if(StringUtils.isEmpty(nxOrxx)){
                if(seconds>-1){
                    this.redissonClient.getBucket(key).set(value, seconds, TimeUnit.SECONDS);
                }else{
                    this.redissonClient.getBucket(key).set(value);
                }
            }else if(nxOrxx.equalsIgnoreCase("nx")){
                if(seconds>-1){
                    this.redissonClient.getBucket(key).setIfAbsent(value, Duration.ofSeconds(seconds));
                }else{
                    this.redissonClient.getBucket(key).setIfAbsent(value);
                }
            }else if(nxOrxx.equalsIgnoreCase("xx")){
                if(seconds>-1){
                    this.redissonClient.getBucket(key).setIfExists(value, seconds, TimeUnit.SECONDS);
                }else{
                    this.redissonClient.getBucket(key).setIfExists(value);
                }
            }

            return true;
        }

        public String get(String key) {
            if(this.redissonClient.getBucket(key).isExists()){
                return this.redissonClient.getBucket(key).get().toString();
            }
            return null;
        }

        public boolean del(String key) {
            if(this.redissonClient.getBucket(key).isExists()){
                return this.redissonClient.getBucket(key).delete();
            }
            return true;
        }

        public boolean exists(String key) {
            return this.redissonClient.getBucket(key).isExists();
        }

        public boolean expire(String key, Long seconds) {
            if(this.redissonClient.getBucket(key).isExists()){
                return this.redissonClient.getBucket(key).expire(Duration.ofSeconds(seconds));
            }
            return false;
        }

        public boolean hset(String key, String field, String value, Long seconds, String nxOrxx) {

            boolean ret=false;
            if(StringUtils.isEmpty(nxOrxx)){
                this.redissonClient.getMap(key).put(field, value);
                ret = true;
            }else if(nxOrxx.equalsIgnoreCase("nx")){
                if(!this.redissonClient.getMap(key).containsKey(field)){
                    this.redissonClient.getMap(key).put(field, value);
                    ret = true;
                }
            }else if(nxOrxx.equalsIgnoreCase("xx")){
                if(this.redissonClient.getMap(key).containsKey(field)){
                    this.redissonClient.getMap(key).put(field, value);
                    ret = true;
                }
            }

            if(ret && seconds >-1){
                this.redissonClient.getMap(key).expire(Duration.ofSeconds(seconds));
                ret = true;
            }

            return ret;
        }

        public Object hget(String key, String field) {
            if(this.redissonClient.getMap(key).containsKey(field)){
                return this.redissonClient.getMap(key).get(field);
            }
            return null;
        }

        public Map<Object, Object> hgetall(String key) {
            if(this.redissonClient.getMap(key).isExists()){
                return this.redissonClient.getMap(key).readAllMap();
            }
            return null;
        }

        public Object hdel(String key, String field) {
            if(this.redissonClient.getMap(key).containsKey(field)){
                return this.redissonClient.getMap(key).remove(field);
            }
            return null;
        }

        public boolean lpush(String key, String value) {
            this.redissonClient.getList(key).add(0, value);
            return true;
        }

        public boolean rpush(String key, String value) {
            return this.redissonClient.getList(key).add(value);
        }

        public List<Object> lrange(String key, Integer start, Integer end) {
            return this.redissonClient.getList(key).range(start, end);
        }

        public Object lpop(String key) {
            if(!this.redissonClient.getList(key).isEmpty()){
                return this.redissonClient.getList(key).remove(0);
            }
            return null;
        }

        public Object rpop(String key) {
            if(!this.redissonClient.getList(key).isEmpty()){
                return this.redissonClient.getList(key).remove( this.redissonClient.getList(key).size()-1);
            }
            return null;
        }

        public boolean sadd(String key, Object value) {
            return this.redissonClient.getSet(key).add(value);
        }

        public Set<Object> smembers(String key) {
            return this.redissonClient.getSet(key).readAll();
        }

        public boolean srem(String key, Object value) {
            return this.redissonClient.getSet(key).remove(value);
        }

        public boolean sismember (String key, Object value) {
            return this.redissonClient.getSet(key).contains(value);
        }

        public boolean zadd(String key, Object value) {
            return this.redissonClient.getSortedSet(key).add(value);
        }

        public Collection<Object> zmembers(String key) {
            return this.redissonClient.getSortedSet(key).readAll();
        }

        public boolean zrem(String key, Object value) {
            return this.redissonClient.getSortedSet(key).remove(value);
        }

        public boolean zismember (String key, Object value) {
            return this.redissonClient.getSortedSet(key).contains(value);
        }


        // 可以根据需求添加更多操作方法，例如哈希、列表、集合等操作

        public void close() {
            try{
                if(this.redissonClient != null){
                    this.redissonClient.shutdown();
                }
            }catch (Exception e){

            }
        }
    }
}
