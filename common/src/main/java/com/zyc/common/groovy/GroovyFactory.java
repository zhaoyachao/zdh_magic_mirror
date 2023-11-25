package com.zyc.common.groovy;

import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.jsr223.GroovyScriptEngineFactory;

import javax.script.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 执行groovy规则工厂
 *
 * 当前工厂未实现缓存功能,后续可通过缓存方向进行性能优化
 */
public class GroovyFactory {

    /**
     * 执行java code, 使用java语法
     * @param javaCode
     * @param param
     * @throws Exception
     */
    public static Object execJavaCode(String javaCode, Object param) throws Exception {
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class<?> clazz = groovyClassLoader.parseClass(javaCode);

        if(clazz != null){
            Object instance = clazz.newInstance();
            if(instance instanceof GroovyHandler){
                Object result = ((GroovyHandler) instance).handler(param);
                return result;
            }
            throw new Exception("执行java code 失败, java code 需要实现接口 com.zyc.common.groovy.GroovyHandler ");
        }
        throw new Exception("load java code class not found");
    }

    /**
     * 执行groovy 脚本, 使用groovy 语法
     * @param script
     * @param params
     * @return
     * @throws ScriptException
     */
    public static Object execExpress(String script, Map<String,Object> params) throws ScriptException {
        GroovyScriptEngineFactory scriptEngineFactory = new GroovyScriptEngineFactory();
        ScriptEngine scriptEngine = scriptEngineFactory.getScriptEngine();
        Bindings bindings = scriptEngine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        if(params != null && params.size()>0){
            for (String key: params.keySet()){
                bindings.put(key, params.get(key));
            }
        }
        return scriptEngine.eval(script);
    }

    /**
     * 执行groovy 函数
     * @param script
     * @param function_name
     * @param params
     * @return
     * @throws ScriptException
     * @throws NoSuchMethodException
     */
    public static Object execExpress(String script, String function_name, Map<String,Object> params) throws ScriptException, NoSuchMethodException {
        GroovyScriptEngineFactory scriptEngineFactory = new GroovyScriptEngineFactory();
        ScriptEngine scriptEngine = scriptEngineFactory.getScriptEngine();
        Bindings bindings = scriptEngine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        List<String> list = new ArrayList<>();

        if(params != null && params.size()>0){
            for (String key: params.keySet()){
                bindings.put(key, params.get(key));
                list.add(params.get(key).toString());
            }
        }
        scriptEngine.eval(script);
        return ((Invocable)scriptEngine).invokeFunction(function_name, list.toArray());
    }

    /**
     *
     * @param javaCode
     * @param param
     * @return
     * @throws Exception
     */
    public static Object execJavaCode(String javaCode, Map<String,Object> param) throws Exception {
        return execJavaCode(javaCode, (Object)param);
    }
}
