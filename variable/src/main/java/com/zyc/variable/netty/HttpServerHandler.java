package com.zyc.variable.netty;


import com.zyc.common.util.JsonUtil;
import com.zyc.variable.service.FilterService;
import com.zyc.variable.service.VariableService;
import com.zyc.variable.service.impl.FilterServiceImpl;
import com.zyc.variable.service.impl.VariableServiceImpl;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HttpServerHandler extends HttpBaseHandler {
    Logger logger= LoggerFactory.getLogger(HttpServerHandler.class);

    //单线程线程池，同一时间只会有一个线程在运行,保证加载顺序
    private ThreadPoolExecutor threadpool = new ThreadPoolExecutor(
            1, // core pool size
            1, // max pool size
            500, // keep alive time
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>()
            );

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        FullHttpRequest request = (FullHttpRequest)msg;
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        HttpResponse response = diapathcer(request);
        if (keepAlive) {
            response.headers().set(Connection, KeepAlive);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.writeAndFlush(defaultResponse(serverErr)).addListener(ChannelFutureListener.CLOSE);
    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Map<String,Object> getBody(String content){
        return JsonUtil.toJavaBean(content, Map.class);
    }

    private Map<String,Object> getParam(String uri) throws UnsupportedEncodingException {
        Map<String, Object> map = new HashMap<>();
        String path = URLDecoder.decode(uri, chartSet);
        String cont = path.substring(path.lastIndexOf("?") + 1);
        if (cont.contains("=")){
            List<String> params = Arrays.stream(cont.split("&|&&")).map(e -> e.trim()).collect(Collectors.toList());
            for (String param : params) {
                String[] kv = param.split("=", 2);
                map.put(kv[0], kv[1]);
            }
        }
        return map;
    }

    private Map<String,Object> getReqContent(FullHttpRequest request) throws UnsupportedEncodingException {

        if(request.method().name().equalsIgnoreCase(HttpMethod.GET.name())){
            return getParam(request.uri());
        }else if(request.method().name().equalsIgnoreCase(HttpMethod.POST.name())){
            return getBody(request.content().toString(CharsetUtil.UTF_8));
        }else if(request.method().name().equalsIgnoreCase(HttpMethod.PUT.name())){
            Map<String,Object> map = getParam(request.uri());
            map.putAll(getBody(request.content().toString(CharsetUtil.UTF_8)));
            return map;
        }else if(request.method().name().equalsIgnoreCase(HttpMethod.DELETE.name())){
            Map<String,Object> map = getParam(request.uri());
            map.putAll(getBody(request.content().toString(CharsetUtil.UTF_8)));
            return map;
        }else if(request.method().name().equalsIgnoreCase(HttpMethod.PATCH.name())){
            Map<String,Object> map = getParam(request.uri());
            map.putAll(getBody(request.content().toString(CharsetUtil.UTF_8)));
            return map;
        }
        return null;
    }


    public HttpResponse diapathcer(FullHttpRequest request) throws UnsupportedEncodingException {
        //生成请求ID
        String request_id = UUID.randomUUID().toString();
        String uri = URLDecoder.decode(request.uri(), chartSet);
        String method = request.method().name();
        logger.info("request:{}, 接收到请求:{}, 请求类型:{}", request_id, uri, method);
        try{
            //解析参数
            Map<String,Object> param = getReqContent(request);
            String resp = "";
            //根据uri 匹配数据库中mock数据
            String url=uri.split("\\?")[0];
            if(url.startsWith("/api/v1/variable")){
                resp = variable(param).toString();
            }else if(url.startsWith("/api/v1/all")){
                resp = JsonUtil.formatJsonString(all(param));
            }else if(url.startsWith("/api/v1/filter")){
                resp = JsonUtil.formatJsonString(filter(param));
            }else if(url.startsWith("/api/v1/hitfilter")){
                resp = String.valueOf(filterIsHit(param));
            }
            logger.info("request:{}, uri:{}, request method:{}, response:{}", request_id, uri, method, resp);
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(resp.getBytes())
            );
            response.headers().setInt(ContentLength, response.content().readableBytes());

            return response;

        }catch (Exception e){
            logger.error("request:{}, uri:{}, request method:{}, error:{} ", request_id, uri, method, e);
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.EXPECTATION_FAILED,
                    Unpooled.wrappedBuffer(e.getMessage().getBytes())
            );
            response.headers().setInt(ContentLength, response.content().readableBytes());

            return response;
        }
    }

    private Object variable(Map<String,Object> param){
        String uid = param.get("uid").toString();
        String variable = param.get("variable").toString();
        String product_code = param.get("product_code").toString();
        VariableService variableService = new VariableServiceImpl();
        return variableService.get(product_code, uid, variable);
    }

    private Map<String,String> all(Map<String,Object> param){
        String uid = param.get("uid").toString();

        VariableService variableService = new VariableServiceImpl();
        String product_code = param.get("product_code").toString();
        if(param.containsKey("variables")){
            List<String> variables = (List<String>)param.get("variables");
            return variableService.getMul(product_code, variables, uid);
        }
        return  variableService.getAll(product_code, uid);
    }

    private Map<String,String> filter(Map<String,Object> param){
        String uid = param.get("uid").toString();
        String product_code = param.get("product_code").toString();
        FilterService filterService = new FilterServiceImpl();
        return filterService.getFilter(uid);
    }

    private boolean filterIsHit(Map<String,Object> param){
        String uid = param.get("uid").toString();
        String filter_code = param.get("filter_code").toString();
        String product_code = param.get("product_code").toString();
        FilterService filterService = new FilterServiceImpl();
        return filterService.isHit(uid, filter_code);
    }
}
