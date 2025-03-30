package com.zyc.common.util;

import com.zyc.common.dao.ZdhLogsMapper;
import com.zyc.common.entity.ZdhLogs;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Timestamp;

public class LogUtil {

    public static SqlSession sqlSession;

    public static String logLock="LOG_LOCK";

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
            if(sqlSession == null){
                synchronized (logLock.intern()){
                    if(sqlSession == null){
                        sqlSession = MybatisUtil.getSqlSession();
                    }
                }
            }
            ZdhLogsMapper zdhLogsMapper = sqlSession.getMapper(ZdhLogsMapper.class);
            zdhLogsMapper.insert(zdhLogs);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

        }
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
}
