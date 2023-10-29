package com.zyc.common.redis;

import org.redisson.api.RedissonClient;

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
    public Object hGet(String key, String secondKey) {
        return redissonClient.getMap(key).get(secondKey);
    }

    @Override
    public void hSet(String key, String secondKey, Object value) {
        redissonClient.getMap(key).put(secondKey, value);
    }

    @Override
    public Map<Object, Object> hGetAll(String key) {
        return redissonClient.getMap(key).readAllMap();
    }

    @Override
    public String getKey(String key) {
        return null;
    }
}
