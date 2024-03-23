package com.zyc.common.redis;

import org.redisson.api.RLock;

import java.util.Map;

public interface RedisClient {

    public Object get(String key);

    public void set(String key, Object value);

    public Object hGet(String key, String secondKey);

    public void hSet(String key, String secondKey, Object value);

    public Map<Object, Object> hGetAll(String key);

    public String getKey(String key);

    public void close();

    public RLock rLock(String lockName);

}
