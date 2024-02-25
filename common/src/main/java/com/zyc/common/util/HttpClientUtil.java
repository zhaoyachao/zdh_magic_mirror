package com.zyc.common.util;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import java.util.Map;

public class HttpClientUtil {

    private final static HttpClientBuilder httpClientBuilder = HttpClients.custom();

    static {
        //https 协议工厂
        SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(SSLContexts.createSystemDefault(),
                new String[]{"TLSv1", "TLSv1.2"},
                null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        //创建注册对象
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.INSTANCE)
                .register("https", sslFactory)
                .build();
        //创建连接池 创建ConnectionManager接口new (连接池)
        PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager(registry);
        //设置连接池最大连接数
        pool.setMaxTotal(2);
        //设置每个路由默认多少链接
        pool.setDefaultMaxPerRoute(2);
        //设置连接池属性
        httpClientBuilder.setConnectionManager(pool);
        RequestConfig requestConfig = RequestConfig.custom()
                //创建链接（TCP协议的三次握手）超时时间
                .setConnectTimeout(60000)
                //响应 获取响应内容超时时间
                .setSocketTimeout(60000)
                //从链接池获取链接的超时时间
                .setConnectionRequestTimeout(30000)
                .build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);

        //设置连接池为共享的
        httpClientBuilder.setConnectionManagerShared(true);

    }

    /**
     * post请求
     */
    public static String postJson(String url, Map<String, Object> params) throws Exception {
        //不是每次创建新的HttpClient，而是从连接池中获取HttpClient对象
        CloseableHttpClient httpClient = httpClientBuilder.build();
        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        String jsonString = JSON.toJSONString(params);
        post.setEntity(new StringEntity(jsonString, "UTF-8"));
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (statusCode > 400) {
                throw new Exception(EntityUtils.toString(response.getEntity()));
            }
            return EntityUtils.toString(entity, "UTF-8");
        } catch (Exception e) {
            throw e;
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public static String postJson(String url,String json) throws Exception {
        //不是每次创建新的HttpClient，而是从连接池中获取HttpClient对象
        CloseableHttpClient httpClient = httpClientBuilder.build();
        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        post.setEntity(new StringEntity(json, "UTF-8"));
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(post);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (statusCode > 400) {
                throw new Exception(EntityUtils.toString(response.getEntity()));
            }
            return EntityUtils.toString(entity, "UTF-8");
        } catch (Exception e) {
            throw e;
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                }
            }
        }
    }

}