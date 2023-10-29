package com.zyc.common.queue;

import java.util.Map;
import java.util.Properties;

public interface QueueHandler {
    public Map<String,Object> handler();

    public void setProperties(Properties properties);
}
