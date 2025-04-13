package com.zyc.magic_mirror.common.http;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpServerTest {

    @Test
    public void registerAction() {

        HttpServer httpServer = new HttpServer();

        String url = "/api/v1/test1";
        String regex = "^/api/v1/.*$";
        httpServer.registerAction(regex, new HttpAction() {
            @Override
            public String getUri() {
                return "/api/v1/test1";
            }

            @Override
            public void before(Map<String, Object> param, String signKey) {

            }

            @Override
            public Object call(Map<String, Object> param) throws Exception {
                return null;
            }
        });

        boolean isFind=false;
        for (Map.Entry<Pattern, HttpAction> entry: HttpServer.regexActions.entrySet()){
            Matcher matcher = entry.getKey().matcher(url);
            if(matcher.matches()){
                isFind=true;
                String findUrl =entry.getValue().getUri();
                System.out.println(findUrl);
                break;
            }
        }

        Assert.assertTrue("正则匹配不成功",isFind);
    }
}