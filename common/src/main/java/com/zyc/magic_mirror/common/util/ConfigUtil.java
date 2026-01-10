package com.zyc.magic_mirror.common.util;

import java.util.Properties;

public class ConfigUtil {
    private static Properties config = new Properties();

    public static String ZDH_PUSHX_BASE_URL = "zdh.pushx.base.url";
    public static String ZDH_PUSHX_SERVICE_KEY = "zdh.pushx.service.key";

    public static void init(Properties properties) {
        config = properties;
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
