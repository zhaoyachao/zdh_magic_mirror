package com.zyc.common.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * http server
 *
 * example:
 *
 * HttpServer httpServer = new HttpServer();
 * httpServer.registerAction("/api/v1/xxx", httpAction);
 *
 * httpServer.start(properties)
 */
public class HttpServer {
    private Logger logger= LoggerFactory.getLogger(HttpServer.class);

    public static String signKey;
    public static Properties properties;
    public static Map<String, HttpAction> actions = new ConcurrentHashMap<>();

    public void registerAction(String uri, HttpAction httpAction){
        logger.info("HttpServer注册路由,uri: {},action: {}", uri, httpAction.getClass().getName());
        actions.put(uri, httpAction);
    }

    public void start(Properties properties) throws Exception {
        HttpServer.properties = properties;
        String host=properties.getProperty("host");
        String port=properties.getProperty("port");
        if(!properties.containsKey("service.key")){
            throw new Exception("HttpServer缺失service.key参数");
        }
        HttpServer.signKey = properties.getProperty("service.key");

        int bossSize = Integer.valueOf(properties.getOrDefault("boss_size", 1).toString());
        int workerSize = Integer.valueOf(properties.getOrDefault("worker_size", 10).toString());
        int contentLength = Integer.valueOf(properties.getOrDefault("content_length", 1024 * 1024).toString());

        bind(host,port, contentLength, bossSize, workerSize);
    }


    public void bind(String host, String port, int contentLength, int bossSize, int workerSize) throws InterruptedException {
        //配置服务端线程池组
        //用于服务器接收客户端连接
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(bossSize);
        //用户进行SocketChannel的网络读写
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerSize);

        try {
            //是Netty用户启动NIO服务端的辅助启动类，降低服务端的开发复杂度
            ServerBootstrap bootstrap = new ServerBootstrap();
            //将两个NIO线程组作为参数传入到ServerBootstrap
            bootstrap.group(bossGroup, workerGroup)
                    //创建NioServerSocketChannel
                    .channel(NioServerSocketChannel.class)
                    //绑定I/O事件处理类
                    .childHandler(new ChannelInitializer<SocketChannel>(){
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new HttpServerCodec());
                            //HttpObjectAggregator解码器 将多个消息对象转换为full
                            ch.pipeline().addLast("aggregator", new HttpObjectAggregator(contentLength));
                            //压缩
                            ch.pipeline().addLast("deflater", new HttpContentCompressor());
                            ch.pipeline().addLast(new HttpServerHandler());
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            logger.info("HttpServer启动完成,host: {},port: {}", host, port);
            //绑定端口，调用sync方法等待绑定操作完成
            ChannelFuture channelFuture = bootstrap.bind(Integer.parseInt(port)).sync();
            //等待服务关闭
            channelFuture.channel().closeFuture().sync();
        } finally {
            //优雅的退出，释放线程池资源
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}
