package com.zyc.magic_mirror.common.groovy;

import java.util.Map;

/**
 * 5.3.3及之后版本
 */
public class JavaDemoHandler implements GroovyHandler {

    @Override
    public Object handler(Object param, Map<String,Object> out) {
        Map<String, Object> in = (Map<String, Object>)param;
        if(Long.parseLong(in.get("strategy_instance_id").toString())>0){
            out.put("out_rs", in.get("rs"));
        }
        return out;
    }
}
