package com.zyc.magic_mirror.common.util;

import cn.hutool.core.util.IdUtil;
import org.slf4j.MDC;

/**
 * logId工具类，用于生成和管理logId
 */
public class LogIdUtil {

    /**
     * 生成logId并设置到MDC中
     * @return 生成的logId
     */
    public static String generateAndSet() {
        String logId = IdUtil.getSnowflake().nextIdStr();
        MDC.put(Const.LOG_ID, logId);
        return logId;
    }

    /**
     * 设置指定的logId到MDC中
     * @param logId 要设置的logId
     */
    public static void set(String logId) {
        if (logId != null) {
            MDC.put(Const.LOG_ID, logId);
        }
    }

    /**
     * 从MDC中获取当前的logId
     * @return 当前的logId，如果不存在则返回null
     */
    public static String get() {
        return MDC.get(Const.LOG_ID);
    }

    /**
     * 清理MDC中的logId
     */
    public static void clear() {
        MDC.remove(Const.LOG_ID);
    }

    /**
     * 生成新的logId并替换当前MDC中的logId
     * @return 新生成的logId
     */
    public static String replace() {
        clear();
        return generateAndSet();
    }
}