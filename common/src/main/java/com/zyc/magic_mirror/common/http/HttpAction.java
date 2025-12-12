package com.zyc.magic_mirror.common.http;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class HttpAction {

    private final static Logger logger = LoggerFactory.getLogger(HttpAction.class);

    /**
     * 默认开启签名验证
     * @return
     */
    public boolean isEnableSign(){
        return true;
    }

    public abstract String getUri();

    /**
     * 做前置检查,签名验证等
     * @param param
     */
    public abstract void before(Map<String, Object> param, String signKey);

    public abstract Object call(Map<String, Object> param) throws Exception;

    public HttpBaseResponse execute(Map<String, Object> param){
        HttpBaseResponse httpBaseResponse = new HttpBaseResponse();
        try{
            httpBaseResponse.setCode(0);
            httpBaseResponse.setMsg("success");
            if(isEnableSign()){
                signCheck(param, HttpServer.signKey);
            }
            before(param, HttpServer.signKey);
            Object obj = call(param);
            httpBaseResponse.setData(obj);
            return httpBaseResponse;
        }catch (Exception e){

            httpBaseResponse.setCode(-1);
            httpBaseResponse.setMsg(e.getMessage());
            return httpBaseResponse;
        }
    }

    public void check(Map<String, List<String>> rules, Map<String, Object> param) throws Exception {
        for(Map.Entry<String, List<String>> entry: rules.entrySet()){
            String key = entry.getKey();
            for(String rule: entry.getValue()){
                if(rule.equalsIgnoreCase("isEmpty")){
                    //为空判断
                    if(!param.containsKey(key) || Objects.isNull(param.get(key)) || StringUtils.isEmpty(param.get(key).toString())){
                        throw new Exception("param "+key+" is empty");
                    }
                }else if(rule.equalsIgnoreCase("isNull")){
                    //null判断
                    if(!param.containsKey(key) || Objects.isNull(param.get(key))){
                        throw new Exception("param "+key+" is null");
                    }
                }
            }

        }

    }

    public void signCheck(Map<String, Object> param, String signKey) throws Exception {
        String signStr = generatSign(param, signKey);
        if(Objects.isNull(param.get("sign"))){
            throw new Exception("sign 验证失败");
        }

        if(!param.get("sign").toString().equals(signStr)){
            throw new Exception("sign 验证失败");
        }
    }

    public static String generatSign(Map<String, Object> param, String signKey){
        try{
            List<String> keys = Lists.newArrayList(param.keySet());
            keys.sort(Comparator.reverseOrder());
            StringBuilder sb = new StringBuilder();
            for (String key: keys){
                if(key.equalsIgnoreCase("sign")){
                    continue;
                }
                if(Objects.isNull(param.get(key))){
                    continue;
                }

                if(!isWrapperClass(param.get(key).getClass())){
                    continue;
                }
                sb.append("&").append(key).append("=").append(param.get(key));
            }
            sb.append(signKey);
            String signStr = sb.toString().substring(1);

            MessageDigest md = MessageDigest.getInstance("MD5");
            // 更新数据
            byte[] messageDigest = md.digest(signStr.getBytes());
            // 将字节数组转换为十六进制字符串
            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                String hex = Integer.toHexString(0xFF & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        }catch (Exception e){
           logger.error("HTTP action error: {}", e.getMessage(), e);
        }
        return null;
    }
    public static boolean isWrapperClass(Class<?> clazz) {
        return clazz != null && (
                clazz == Byte.class ||
                        clazz == Short.class ||
                        clazz == Integer.class ||
                        clazz == Long.class ||
                        clazz == Float.class ||
                        clazz == Double.class ||
                        clazz == Character.class ||
                        clazz == Boolean.class
        );
    }

}
