package com.zyc.magic_mirror.common.redis;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.Map;

public class RedisClientImpl implements RedisClient{

    private RedissonClient redissonClient;

    public RedisClientImpl(RedissonClient redissonClient){
        this.redissonClient = redissonClient;
    }

    @Override
    public Object get(String key) {
        return redissonClient.getBucket(key).get();
    }

    @Override
    public void set(String key, Object value) {
        redissonClient.getBucket(key).set(value);
    }

    @Override
    public boolean del(String key) {
        return redissonClient.getBucket(key).delete();
    }

    @Override
    public Object hGet(String key, String secondKey) {
        return redissonClient.getMap(key).get(secondKey);
    }

    @Override
    public void hSet(String key, String secondKey, Object value) {
        redissonClient.getMap(key).put(secondKey, value);
    }

    @Override
    public void hDel(String key, String secondKey) {
        redissonClient.getMap(key).remove(secondKey);
    }

    @Override
    public Map<Object, Object> hGetAll(String key) {
        return redissonClient.getMap(key).readAllMap();
    }

    @Override
    public String getKey(String key) {
        return null;
    }

    @Override
    public void expire(String key, Long second) {
        redissonClient.getBucket(key).expire(Duration.ofSeconds(second));
    }

    @Override
    public boolean isExists(String key) {
        return redissonClient.getBucket(key).isExists();
    }

    @Override
    public void close() {
        redissonClient.shutdown();
    }

    @Override
    public RLock rLock(String lockName){
        try{
            RLock rLock = redissonClient.getLock(lockName);

            return rLock;
        }catch (Exception e){
            throw e;
        }
    }
}
