package com.zyc.magic_mirror.common.redis;

import org.redisson.api.RLock;

import java.util.Map;

public interface RedisClient {

    public Object get(String key);

    public void set(String key, Object value);

    public boolean del(String key);

    public Object hGet(String key, String secondKey);

    public void hSet(String key, String secondKey, Object value);

    public void hDel(String key, String secondKey);

    public Map<Object, Object> hGetAll(String key);

    public String getKey(String key);

    /**
     *
     * @param key
     * @param second 过期时间, 单位秒
     */
    public void expire(String key, Long second);

    public boolean isExists(String key);

    public void close();

    public RLock rLock(String lockName);

}
