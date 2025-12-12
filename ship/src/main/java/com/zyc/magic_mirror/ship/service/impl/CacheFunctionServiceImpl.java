package com.zyc.magic_mirror.ship.service.impl;

import cn.hutool.core.lang.JarClassLoader;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ClassLoaderUtil;
import com.google.common.collect.Maps;
import com.zyc.magic_mirror.common.entity.FunctionInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.ship.dao.FunctionMapper;
import com.zyc.magic_mirror.ship.service.FunctionService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheFunctionServiceImpl extends BaseServiceImpl implements FunctionService {

    private static Logger logger= LoggerFactory.getLogger(CacheFunctionServiceImpl.class);

    public static Map<String,FunctionInfo> cache=new HashMap<>();
    public static Map<String,Object> cacheFunctionInstance=new HashMap<>();

    @Override
    public List<FunctionInfo> selectAll() {
        return executeReadOnly((sqlSession) -> {
            FunctionMapper functionMapper = sqlSession.getMapper(FunctionMapper.class);
            return functionMapper.selectAllNotDelete();
        });
    }

    @Override
    public FunctionInfo selectByFunctionCode(String function_code) {
        try {

            if(cache.containsKey(function_code)){
                return cache.get(function_code);
            }
            return null;
        } catch (Exception e) {
            logger.error("ship service selectByFunctionCode error: ", e);
            return null;
        }finally {

        }
    }


    public void schedule(){
        //定时加载函数信息

        try {
            List<FunctionInfo> rows = executeReadOnly((sqlSession) -> {
                FunctionMapper functionMapper = sqlSession.getMapper(FunctionMapper.class);

                return functionMapper.selectAllNotDelete();
            });

            Map<String,FunctionInfo> maps = Maps.newHashMap();
            for(FunctionInfo row : rows) {
                maps.put(row.getFunction_name(), row);

                try{
                    String function_name = row.getFunction_name();
                    String function_class = row.getFunction_class();
                    String function_load_path = row.getFunction_load_path();
                    String function_script = row.getFunction_script();
                    List<Object> jsonArray = row.getParam_json_object();

                    if(!StringUtils.isEmpty(function_class)){
                        String[] function_packages = function_class.split(",");
                        String clsName = ArrayUtil.get(function_packages, function_packages.length-1);
                        String clsInstanceName = StringUtils.uncapitalize(clsName);
                        //加载三方工具类
                        if(!StringUtils.isEmpty(function_load_path)){
                            JarClassLoader jarClassLoader = JarClassLoader.loadJar(new File(function_load_path));
                            Class cls = jarClassLoader.loadClass(function_class);
                            Object clsInstance = cls.newInstance();
                            cacheFunctionInstance.put(function_name, clsInstance);
                        }else{
                            Object clsInstance = ClassLoaderUtil.loadClass(function_class).newInstance();
                            cacheFunctionInstance.put(function_name, clsInstance);
                        }
                    }
                }catch (Exception e){
                    logger.error("ship service schedule function error: ", e);
                }
            }
            cache = maps;
        } catch (Exception e) {
            logger.error("ship service schedule error: ", e);
        }finally {

        }
    }

}
