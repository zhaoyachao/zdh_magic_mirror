package com.zyc.variable.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpBaseHandler extends ChannelInboundHandlerAdapter {
    AsciiString ContentType = AsciiString.cached("Content-Type");
    AsciiString ContentLength = AsciiString.cached("Content-Length");
    AsciiString Connection = AsciiString.cached("Connection");
    AsciiString KeepAlive = AsciiString.cached("keep-alive");
    String noParam = "{\"code\":500,\"msg\":\"no params\"}";
    String noService = "{\"code\":500,\"msg\":\"no match reportService\"}";
    String noUri = "{\"code\":500,\"msg\":\"request uri is wrong\"}";
    String unknownParam = "{\"code\":500,\"msg\":\"unknown cmd\"}";
    String cmdOk = "{\"code\":200,\"msg\":\"command executed\"}";
    String execErr = "{\"code\":500,\"msg\":\"command execute error\"}";
    String serverErr = "{\"code\":500,\"msg\":\"server error\"}";
    String cacheIsNull = "{\"code\":501,\"msg\":\"model cache is null\"}";
    String chartSet = "utf-8";


    public HttpResponse defaultResponse(String respContent) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(respContent.getBytes())
        );
        response.headers().set(ContentType, "application/json");
        response.headers().setInt(ContentLength, response.content().readableBytes());
        return response;
    }


    public Map<String, String> parseGetParam(String uri) throws UnsupportedEncodingException {
        Map<String, String> map = new HashMap<>();
        String[] array = URLDecoder.decode(uri, chartSet).split("\\?");
        if (array.length > 1) {
            List<String> params = Arrays.stream(array[1].split("&")).map(e -> e.trim()).collect(Collectors.toList());

            for (String param : params) {
                String[] kv = param.split("=", 2);
                map.put(kv[0], kv[1]);
            }
        }
        return map;
    }
}
