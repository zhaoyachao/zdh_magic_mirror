package com.zyc.ship.common;

public class Const {

    public static String ONLINE_MANAGER="online_manager";//实时接入流量,准实时经营
    public static String ONLINE_RISK="online_risk";

    public static String LABEL_PARAM_PRE = "tag_";

    public static int ERROR_CODE_NOT_FOUND_STRATEGY_GROUP_INSTANCE_FLOW=1001;//策略组实例未配置分流
    public static int ERROR_CODE_NOT_HIT_STRATEGY_GROUP_INSTANCE=1002;//未命中分流

    public static String TN_TYPE_RELATIVE="relative";
    public static String TN_TYPE_ABSOLUTE="absolute";

    public static String ONLINE_RISK_IS_STOP_KEY = "ONLINE_RISK_IS_STOP_KEY";

    public static String SHIP_ONLINE_RISK_LOG_QUEUE = "SHIP_ONLINE_RISK_LOG_QUEUE";

    public static String SHIP_ONLINE_MANAGER_LOG_QUEUE = "SHIP_ONLINE_MANAGER_LOG_QUEUE";
}
