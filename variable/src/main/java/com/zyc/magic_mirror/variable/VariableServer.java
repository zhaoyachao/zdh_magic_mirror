package com.zyc.magic_mirror.variable;

import com.zyc.magic_mirror.common.http.HttpServer;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.variable.action.*;
import com.zyc.magic_mirror.variable.service.impl.CacheLabelServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class VariableServer {

    public static Logger logger= LoggerFactory.getLogger(VariableServer.class);

    public static void main(String[] args) {

        try{
            String conf_path = VariableServer.class.getClassLoader().getResource("application.properties").getPath();

            Properties properties = new Properties();
            InputStream inputStream= VariableServer.class.getClassLoader().getResourceAsStream("application.properties");
            properties.load(inputStream);
            File confFile = new File("conf/application.properties");
            if(confFile.exists()){
                conf_path = confFile.getPath();
                inputStream = new FileInputStream(confFile);
                properties.load(inputStream);
            }
            logger.info("加载配置文件路径:{}", conf_path);

            JedisPoolUtil.connect(properties);

            CacheLabelServiceImpl cacheLabelService = new CacheLabelServiceImpl();

            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,
                    1, 1000*60*60,
                    TimeUnit.MICROSECONDS, new LinkedBlockingDeque<>(),
                    Executors.defaultThreadFactory());
            Thread schedule = new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        cacheLabelService.schedule();
                        Thread.sleep(1000*60);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            });
            schedule.setName("variable_schedule");
            threadPoolExecutor.execute(schedule);


            HttpServer httpServer = new HttpServer();
            FilterAction filterAction = new FilterAction();
            FilterHitAction filterHitAction = new FilterHitAction();
            VariableAction variableAction = new VariableAction();
            VariableAllAction variableAllAction = new VariableAllAction();
            VariableUpdateAction variableUpdateAction = new VariableUpdateAction();

            httpServer.registerAction(filterAction.getUri(), filterAction);
            httpServer.registerAction(filterHitAction.getUri(), filterHitAction);
            httpServer.registerAction(variableAction.getUri(), variableAction);
            httpServer.registerAction(variableAllAction.getUri(), variableAllAction);
            httpServer.registerAction(variableUpdateAction.getUri(), variableUpdateAction);

            httpServer.start(properties);

        }catch (Exception e){
            logger.error("variable server error: ", e);
        }

    }
}
