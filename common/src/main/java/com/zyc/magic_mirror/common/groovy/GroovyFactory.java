package com.zyc.magic_mirror.common.groovy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zyc.magic_mirror.common.util.MD5Util;
import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.jsr223.GroovyScriptEngineFactory;

import javax.script.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 执行groovy规则工厂
 *
 * 当前工厂未实现缓存功能,后续可通过缓存方向进行性能优化
 */
public class GroovyFactory {

    public static final GroovyScriptEngineFactory scriptEngineFactory = new GroovyScriptEngineFactory();

    public static final int optimizationLevel = 3;

    private static final ScriptEngine scriptEngine = scriptEngineFactory.getScriptEngine();

    private static Cache<String, CompiledScript> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES) // 最后一次访问后 60 分钟过期
            .expireAfterWrite(120, TimeUnit.MINUTES) // 最后一次写入后 120 分钟过期（取较早）
            .build();

    public static void optimization(){
        scriptEngine.getContext().setAttribute("groovy.compile.optimization.level",
        optimizationLevel,
        ScriptContext.ENGINE_SCOPE);
        scriptEngine.getContext().setAttribute(
        "groovy.compile.static",
        true,  // 启用静态编译优化
        ScriptContext.ENGINE_SCOPE);
        scriptEngine.getContext().setAttribute("groovy.auto.imports", false, ScriptContext.ENGINE_SCOPE);
    }

    static {
        optimization();
    }

    public static ScriptEngine getEngine(){
        return scriptEngine;
    }

    /**
     * 执行java code, 使用java语法
     * @param javaCode
     * @param param
     * @throws Exception
     */
    public static Object execJavaCode(String javaCode, Object param) throws Exception {
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        try{
            Class<?> clazz = groovyClassLoader.parseClass(javaCode);

            if(clazz != null){
                Object instance = clazz.newInstance();
                if(instance instanceof GroovyHandler){
                    Object result = ((GroovyHandler) instance).handler(param, new HashMap<>());
                    return result;
                }
                throw new Exception("执行java code 失败, java code 需要实现接口 com.zyc.common.groovy.GroovyHandler ");
            }
            throw new Exception("load java code class not found");
        }catch (Exception e){
            throw e;
        }finally {
            groovyClassLoader.close();
        }

    }

    /**
     * 执行groovy 脚本, 使用groovy 语法
     * @param script
     * @param params
     * @return
     * @throws ScriptException
     */
    public static Object execExpress(String script, Map<String,Object> params) throws ScriptException {
        return execExpress(script, params, false);
    }

    /**
     * 执行groovy 脚本, 使用groovy 语法
     * @param script
     * @param params
     * @param cache 是否使用编译模式
     * @return
     * @throws ScriptException
     */
    public static Object execExpress(String script, Map<String,Object> params, boolean cache) throws ScriptException {
        Bindings bindings = new SimpleBindings();
        if(params != null && params.size()>0){
            for (String key: params.keySet()){
                bindings.put(key, params.get(key));
            }
        }

        if(cache){
            CompiledScript compiledScript = getCompiledScript(script);
            if(compiledScript != null){
                return compiledScript.eval(bindings);
            }
        }
        return getEngine().eval(script, bindings);
    }

    /**
     * 获取预编译脚本（带缓存）
     */
    private static CompiledScript getCompiledScript(String script){
        try{
            // 使用脚本哈希作为缓存键
            String scriptKey = MD5Util.getMD5(script);
            CompiledScript compiledScript = cache.getIfPresent(scriptKey);
            // 缓存命中直接返回
            if (compiledScript!=null) {
                return compiledScript;
            }

            Compilable compilable = (Compilable) getEngine();
            compiledScript = compilable.compile(script);
            cache.put(scriptKey, compiledScript);
            return compiledScript;
        }catch (Exception e){
            return null;
        }
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
        List<String> list = new ArrayList<>();
        Bindings bindings = new SimpleBindings();
        if(params != null && params.size()>0){
            for (String key: params.keySet()){
                bindings.put(key, params.get(key));
                list.add(params.get(key).toString());
            }
        }
        getEngine().eval(script, bindings);
        return ((Invocable)getEngine()).invokeFunction(function_name, list.toArray());
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
