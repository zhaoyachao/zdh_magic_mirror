package com.zyc.magic_mirror.common.util;

/**
 * 常量
 */
public class Const {
    public static String STATUS_INIT="create";
    public static String STATUS_CHECK_DEP="check_dep";
    public static String STATUS_CHECK_DEP_FINISH="check_dep_finish";
    public static String STATUS_WAIT="wait";
    public static String STATUS_SKIP="skip";
    public static String STATUS_ETL="etl";
    public static String STATUS_FINISH="finish";
    public static String STATUS_ERROR="error";
    public static String STATUS_KILL="kill";
    public static String STATUS_KILLED="killed";

    public static String ZDH_STOP_FLAG_KEY="zdh_stop_flag";
    public static String ZDH_LABEL_STOP_FLAG_KEY="zdh_label_stop_flag";
    public static String ZDH_PLUGIN_STOP_FLAG_KEY="zdh_plugin_stop_flag";

    public static String FILE_STATUS_SUCCESS = "1";//成功
    public static String FILE_STATUS_FAIL = "2";//失败
    public static String FILE_STATUS_ALL = "3";//不区分

    public static String LABEL_DOUBLE_CHECK_DEPENDS_QUEUE_NAME="zdh_label_double_check_depends_queue";

    public static String STRATEGY_INSTANCE_RETRY_COUNT = "retry_count";
    public static String STRATEGY_INSTANCE_DOUBLECHECK_TIME = "doublecheck_time";
    public static String STRATEGY_INSTANCE_SUCCESS_NUM = "success_num";
    public static String STRATEGY_INSTANCE_FAILED_NUM = "failed_num";
    public static String STRATEGY_INSTANCE_IS_ASYNC = "is_async";
    public static String STRATEGY_INSTANCE_ASYNC_TASK_ID = "async_task_id";
    public static String STRATEGY_INSTANCE_ASYNC_TASK_STATUS = "async_task_status";
    public static String STRATEGY_INSTANCE_ASYNC_TASK_EXT = "async_task_ext";

    public static String ASYNC_TASK_STATUS_RUNNING = "running";
    public static String ASYNC_TASK_STATUS_FINISH = "finish";
    public static String ASYNC_TASK_STATUS_FAIL = "fail";

    public static String LOG_TYPE_MYSQL = "mysql";
    public static String LOG_TYPE_MONGODB = "mongodb";

}
