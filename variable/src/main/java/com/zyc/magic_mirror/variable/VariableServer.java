package com.zyc.magic_mirror.variable;

import com.zyc.magic_mirror.common.http.HttpAction;
import com.zyc.magic_mirror.common.http.HttpServer;
import com.zyc.magic_mirror.common.http.PackageScanner;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.ConfigUtil;
import com.zyc.magic_mirror.common.util.LogIdUtil;
import com.zyc.magic_mirror.variable.service.impl.CacheLabelServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 变量服务主类
 */
public class VariableServer {

    private static final Logger logger = LoggerFactory.getLogger(VariableServer.class);
    private static final long SCHEDULE_INTERVAL = 60; // 定时任务执行间隔（秒）

    public static void main(String[] args) {
        LogIdUtil.generateAndSet();
        logger.info("变量服务启动...");
        try {
            // 加载配置文件
            ConfigUtil.load();
            
            // 初始化Redis连接
            initRedis();
            
            // 启动定时任务
            startScheduleTask();
            
            // 启动HTTP服务器
            startHttpServer();
            
            logger.info("变量服务启动成功");
            
        } catch (Exception e) {
            logger.error("变量服务启动失败: ", e);
            System.exit(1);
        }finally {
            LogIdUtil.clear();
        }
    }
    
    /**
     * 初始化Redis连接
     */
    private static void initRedis() {
        try {
            JedisPoolUtil.connect(ConfigUtil.getConfig());
            logger.info("Redis连接成功");
        } catch (Exception e) {
            logger.error("Redis连接失败: ", e);
            throw new RuntimeException("Redis连接初始化失败", e);
        }
    }
    
    /**
     * 启动定时任务
     */
    private static void startScheduleTask() {
        CacheLabelServiceImpl cacheLabelService = new CacheLabelServiceImpl();
        
        // 使用ScheduledExecutorService实现定时任务
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("variable_schedule");
            thread.setDaemon(false); // 设置为非守护线程，确保任务完成
            return thread;
        });
        
        // 延迟0秒后开始执行，每SCHEDULE_INTERVAL秒执行一次
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LogIdUtil.generateAndSet();
                cacheLabelService.schedule();
                logger.debug("定时任务执行成功");
            } catch (Exception e) {
                logger.error("定时任务执行失败: ", e);
                // 异常捕获后不会影响下一次任务执行
            }finally {
                LogIdUtil.clear();
            }
        }, 0, SCHEDULE_INTERVAL, TimeUnit.SECONDS);
        
        logger.info("定时任务已启动，执行间隔: {}秒", SCHEDULE_INTERVAL);
    }
    
    /**
     * 启动HTTP服务器
     */
    private static void startHttpServer() {
        try {
            HttpServer httpServer = new HttpServer();
            
            // 自动扫描并注册HTTP Action
            PackageScanner.autoInit("com.zyc.magic_mirror.variable.action", HttpAction.class);
            
            httpServer.start(ConfigUtil.getConfig());
            logger.info("HTTP服务器启动成功");
        } catch (Exception e) {
            logger.error("HTTP服务器启动失败: ", e);
            throw new RuntimeException("HTTP服务器初始化失败", e);
        }
    }
}