package com.zyc.magic_mirror.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * 当前服务统一配置文件加载类
 * 所有的配置均需要从此处获取
 */
public class ConfigUtil {
    private static Logger logger = LoggerFactory.getLogger(ConfigUtil.class);
    private static Properties config = new Properties();

    public static String ZDH_PUSHX_BASE_URL = "zdh.pushx.base.url";
    public static String ZDH_PUSHX_SERVICE_KEY = "zdh.pushx.service.key";
    public static String SERVICE_NAME = "service.name";
    public static String TASK_MAX_NUM = "task.max.num";
    public static String TASK_MAX_TIME = "task.max.time";
    public static String LOG_TYPE = "log.type";
    public static String LOG_MONGODB_URL = "log.mongodb.url";
    public static String LOG_MONGODB_DB = "log.mongodb.db";
    public static String LOG_MONGODB_MAX_POOL_SIZE = "log.mongodb.maxPoolSize";
    public static String LOG_MONGODB_MIN_POOL_SIZE = "log.mongodb.minPoolSize";
    public static String LOG_MONGODB_MAX_WAIT_TIME = "log.mongodb.maxWaitTime";
    public static String FILE_PATH = "file.path";
    public static String FILE_ROCKSDB_PATH = "file.rocksdb.path";
    public static String TASK_SLOT_TOTAL_NUM = "task.slot.total.num";
    public static String TASK_SLOT = "task.slot";
    public static String TASK_SLOT_TIMEOUT = "task.slot.timeout";
    public static String REDIS_HOST = "redis.host";
    public static String REDIS_PORT = "redis.port";
    public static String REDIS_PASSWORD = "redis.password";
    public static String REDIS_MODE = "redis.mode";

    public static void load() {
        try {
            String confPath = ConfigUtil.class.getClassLoader().getResource("application.properties").getPath();
            File externalConfFile = new File("conf/application.properties");
            if (externalConfFile.exists()) {
                confPath = externalConfFile.getPath();
                try (FileInputStream fis = new FileInputStream(externalConfFile)) {
                    config.load(fis);
                }
            }else{
                config.load(ConfigUtil.class.getClassLoader().getResourceAsStream("application.properties"));
            }
            logger.info("加载配置文件路径: {}", confPath);

            if (config.isEmpty()) {
                throw new Exception("找不到有效配置文件");
            }

        } catch (Exception e) {
            logger.error("加载配置文件失败: ", e);
            throw new RuntimeException("找不到有效配置文件");
        }
    }
    public static void loadByProperties(Properties properties) {
        config = properties;
        logger.info("手动加载配置文件成功");
    }

    public static Properties getConfig() {
        return config;
    }

    public static void set(String key, String value) {
        config.setProperty(key, value);
    }

    public static String get(String key) {
        return config.getProperty(key);
    }

    public static String get(String key, String defaultValue) {
        return config.getProperty(key, defaultValue);
    }


}
