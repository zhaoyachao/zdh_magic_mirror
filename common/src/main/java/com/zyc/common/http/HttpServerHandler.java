package com.zyc.common.http;


import com.zyc.common.util.JsonUtil;
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
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

public class HttpServerHandler extends HttpBaseHandler {
    Logger logger= LoggerFactory.getLogger(HttpServerHandler.class);

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
        HttpBaseResponse httpBaseResponse=new HttpBaseResponse();
        logger.info("request:{}, 接收到请求:{}, 请求类型:{}", request_id, uri, method);
        try{
            //解析参数
            Map<String,Object> param = getReqContent(request);

            //根据uri 匹配数据库中mock数据
            String url=uri.split("\\?")[0];
            if(HttpServer.actions.containsKey(url)){
                httpBaseResponse = HttpServer.actions.get(url).execute(param);
            }else{
                httpBaseResponse.setCode(-1);
                httpBaseResponse.setMsg("not found path");
            }

            logger.info("request:{}, uri:{}, request method:{}, response:{}", request_id, uri, method, JsonUtil.formatJsonString(httpBaseResponse));
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(JsonUtil.formatJsonString(httpBaseResponse).getBytes())
            );
            response.headers().set(CONTENT_TYPE, "application/json;charset=utf-8");
            response.headers().setInt(ContentLength, response.content().readableBytes());

            return response;

        }catch (Exception e){
            logger.error("request:{}, uri:{}, request method:{}, error:{} ", request_id, uri, method, e);
            httpBaseResponse.setCode(-1);
            httpBaseResponse.setMsg(e.getMessage());
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.EXPECTATION_FAILED,
                    Unpooled.wrappedBuffer(JsonUtil.formatJsonString(httpBaseResponse).getBytes())
            );
            response.headers().setInt(ContentLength, response.content().readableBytes());

            return response;
        }
    }
}
