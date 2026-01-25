package com.zyc.magic_mirror.common.util;

import com.mongodb.client.MongoCollection;
import com.zyc.magic_mirror.common.dao.ZdhLogsMapper;
import com.zyc.magic_mirror.common.entity.ZdhLogs;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Objects;

public class LogUtil {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LogUtil.class);
    public static String logLock="LOG_LOCK";

    public static String logType = "mysql";//mysql,mongodb

    public static MongoDBUtil mongoDBUtil;

    // 当前日志类名
    private final static String logClassName = LogUtil.class.getName();

    /**
     * 获取最原始被调用的堆栈信息
     */
    private static StackTraceElement getCaller() {

        // 获取堆栈信息
        StackTraceElement[] traceElements = Thread.currentThread()
                .getStackTrace();
        if (null == traceElements) {
            return null;
        }

        // 最原始被调用的堆栈信息
        StackTraceElement caller = null;

        // 循环遍历到日志类标识
        boolean isEachLogFlag = false;

        // 遍历堆栈信息，获取出最原始被调用的方法信息
        // 当前日志类的堆栈信息完了就是调用该日志类对象信息
        for (StackTraceElement element : traceElements) {
            // 遍历到日志类
            if (element.getClassName().equalsIgnoreCase(logClassName)) {
                isEachLogFlag = true;
            }
            // 下一个非日志类的堆栈，就是最原始被调用的方法
            if (isEachLogFlag) {
                //关键: 实际场景需要通过断点来判断是否是自己想要打印的路径
                if (!element.getClassName().equals(logClassName)) {
                    caller = element;
                    break;
                }
            }
        }

        return caller;
    }

    /**
     * 自动匹配请求类名，生成logger对象
     */
    private static Logger log() {
        // 最原始被调用的堆栈对象
        StackTraceElement caller = getCaller();
        // 空堆栈处理
        if (caller == null) {
            return Logger.getLogger(LogUtil.class);
        }

        // 取出被调用对象的类名，并构造一个Logger对象返回
        return Logger.getLogger(caller.getClassName());
    }


    public static void send(ZdhLogs zdhLogs){
        try {
            if(logType.equalsIgnoreCase(Const.LOG_TYPE_MONGODB)){
                MongoCollection<ZdhLogs> zdh_log = mongoDBUtil.getCollection("zdh_log", ZdhLogs.class);
                zdh_log.insertOne(zdhLogs);
            }else{
                SqlSession sqlSession = MybatisUtil.getSqlSession();
                try{
                    ZdhLogsMapper zdhLogsMapper = sqlSession.getMapper(ZdhLogsMapper.class);
                    zdhLogsMapper.insert(zdhLogs);
                }catch (Exception e){
                    throw e;
                }finally{
                    sqlSession.close();
                }

            }
        } catch (IOException e) {
            logger.error("IO error in LogUtil: {}", e.getMessage(), e);
        }finally {

        }
    }

    /**
     * 获取异常的「简洁非空信息」（适合日志打印、用户提示）
     * @param throwable 异常对象
     * @return 非空的简洁异常信息
     */
    public static String getExceptionMessage(Throwable throwable) {
        // 第一步：判空兜底（防止传入null异常）
        if (throwable == null) {
            return "未知异常（Throwable is null）";
        }
        // 第二步：提取当前异常的简洁信息
        String currentMessage = getCurrentExceptionSimpleMessage(throwable);

        // 第三步：提取根源异常的简洁信息（解决包装异常消息为空的问题）
        Throwable rootCause = getRootCause(throwable);
        if (rootCause == null || throwable == rootCause) {
            // 无根源异常，直接返回当前异常信息
            return currentMessage;
        }

        String rootMessage = getCurrentExceptionSimpleMessage(rootCause);
        if (currentMessage.equals(rootMessage)) {
            // 当前异常和根源异常信息一致，避免重复
            return currentMessage;
        }

        // 第四步：拼接当前异常+根源异常信息（更全面）
        return String.format("%s [根源异常：%s]", currentMessage, rootMessage);
    }

    /**
     * 获取异常链的「根源异常」（最底层的真实异常）
     * @param throwable 异常对象
     * @return 根源异常，无则返回null
     */
    private static Throwable getRootCause(Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        Throwable rootCause = throwable;
        // 循环获取cause，直到没有下一个异常
        while (rootCause.getCause() != null && !Objects.equals(rootCause, rootCause.getCause())) {
            rootCause = rootCause.getCause();
        }

        return rootCause == throwable ? null : rootCause;
    }

    /**
     * 提取单个异常的简洁信息（兜底：getMessage()为空则返回类名）
     * @param throwable 单个异常对象
     * @return 非空的单个异常简洁信息
     */
    private static String getCurrentExceptionSimpleMessage(Throwable throwable) {
        if (throwable == null) {
            return "未知异常";
        }

        // 优先获取getMessage()
        String message = throwable.getMessage();
        if (message != null && !message.trim().isEmpty()) {
            return message.trim();
        }

        // getMessage()为空，返回toString()（类名+可能的消息）
        String toString = throwable.toString();
        if (toString != null && !toString.trim().isEmpty()) {
            return toString.trim();
        }

        // 极端情况：返回异常类名
        return throwable.getClass().getName();
    }

    public static void error(String job_id,String task_logs_id, Exception e){
        log().log(LogUtil.class.getName(), Level.ERROR, e.getMessage(), e);
        ZdhLogs zdhLogs=getZdhLogs("ERROR", job_id, task_logs_id, getExceptionMessage(e));
        send(zdhLogs);
    }

    public static void error(String job_id,String task_logs_id, String msg){
        log().log(LogUtil.class.getName(), Level.ERROR, msg, null);
        ZdhLogs zdhLogs=getZdhLogs("ERROR", job_id, task_logs_id, msg);
        send(zdhLogs);
    }

    public static void info(String job_id,String task_logs_id, String msg){
        log().log(LogUtil.class.getName(), Level.INFO, msg, null);
        ZdhLogs zdhLogs=getZdhLogs("INFO", job_id, task_logs_id, msg);
        send(zdhLogs);
    }

    public static void warn(String job_id,String task_logs_id, String msg){
        log().log(LogUtil.class.getName(), Level.INFO, msg, null);
        ZdhLogs zdhLogs=getZdhLogs("WARN", job_id, task_logs_id, msg);
        send(zdhLogs);
    }

    public static void debug(String job_id,String task_logs_id, String msg){
        log().log(LogUtil.class.getName(), Level.DEBUG, msg, null);
        ZdhLogs zdhLogs=getZdhLogs("DEBUG", job_id, task_logs_id, msg);
        send(zdhLogs);
    }

    public static void console(String job_id,String task_logs_id, String msg){
        log().log(LogUtil.class.getName(), Level.INFO, "策略id: "+job_id+", 策略实例id: "+task_logs_id+", "+msg, null);
    }

    public static ZdhLogs getZdhLogs(String level, String job_id,String task_logs_id, String msg){
        ZdhLogs zdhLogs=new ZdhLogs();
        zdhLogs.setLevel(level);
        zdhLogs.setJob_id(job_id);
        zdhLogs.setTask_logs_id(task_logs_id);
        zdhLogs.setMsg(msg);
        zdhLogs.setLog_time(new Timestamp(System.currentTimeMillis()));
        return zdhLogs;
    }

    /**
     * 使用mogodb存储时需要初始化mogodb
     * @param mongodbUrl
     * @param mongodbDb
     * @param maxPoolSize
     * @param minPoolSize
     * @param maxWaitTime
     */
    public static void initMongoDb(String mongodbUrl,String mongodbDb,
                                   int maxPoolSize, int minPoolSize, int maxWaitTime){
        mongoDBUtil = MongoDBUtil.getInstance(mongodbUrl, mongodbDb, maxPoolSize, minPoolSize, maxWaitTime);
    }
}
