package com.zyc.variable;

import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.variable.netty.NettyServer;
import com.zyc.variable.service.impl.CacheLabelServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

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
            schedule.start();

            NettyServer nettyServer=new NettyServer();
            nettyServer.start(properties);

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
