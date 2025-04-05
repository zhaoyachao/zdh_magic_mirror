package com.zyc.magic_mirror.common.groovy;

import java.util.Map;

/**
 * groovy接口
 */
public interface GroovyHandler {
    public Object handler(Object param, Map<String,Object> out);
}
