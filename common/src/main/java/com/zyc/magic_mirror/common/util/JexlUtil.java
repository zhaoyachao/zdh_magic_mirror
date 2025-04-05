package com.zyc.magic_mirror.common.util;

import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.internal.Engine;

import java.util.Map;

/**
 * java 代码执行引擎工具类
 */
public class JexlUtil {

    public static Object execScript(String scriptStr, Map<String,Object> params){
        JexlEngine engine = new Engine();

        JexlContext context = new MapContext();
        if(params.size()>0){
            for (String key : params.keySet()){
                context.set(key, params.get(key));
            }
        }

        JexlScript script = engine.createScript(scriptStr);
        Object execute = script.execute(context);
        return execute;
    }
}
