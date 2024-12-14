package com.zyc.plugin.calculate.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.VarPoolCalculate;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 变量池实现
 */
public class VarPoolCalculateImpl extends BaseCalculate implements VarPoolCalculate {
    private static Logger logger= LoggerFactory.getLogger(VarPoolCalculateImpl.class);

    /**
     * {
     * 	"strategy_instance": [{
     * 		"id": 1274745674495168512,
     * 		"strategy_context": "(and)增加参数",
     * 		"group_id": "1274686648059170816",
     * 		"group_context": "测试变量池",
     * 		"group_instance_id": "1274745674335784960",
     * 		"instance_type": "varpool",
     * 		"start_time": "2024-08-18 15:04:10",
     * 		"end_time": "2024-08-18 11:06:24",
     * 		"jsmind_data": "{\"varpool_param\":\"[{\\\"varpool_domain\\\":\\\"domain1\\\",\\\"varpool_code\\\":\\\"name\\\"}]\",\"project_scene\":\"\",\"project_code\":\"\",\"type\":\"varpool\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"增加参数\",\"positionX\":271,\"rule_id\":\"1274686544979955712\",\"positionY\":205,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)增加参数\",\"more_task\":\"varpool\",\"id\":\"1274685896251150336\",\"data_status\":\"1\",\"divId\":\"1274685896251150336\",\"depend_level\":\"0\"}",
     * 		"owner": "zyc",
     * 		"is_delete": "0",
     * 		"create_time": "2024-08-18 15:04:12",
     * 		"update_time": "2024-08-18 15:04:12",
     * 		"expr": "1d",
     * 		"misfire": "0",
     * 		"priority": "",
     * 		"status": "check_dep_finish",
     * 		"quartz_time": "2024-08-18 11:09:39",
     * 		"use_quartz_time": "off",
     * 		"time_diff": "0",
     * 		"schedule_source": "2",
     * 		"cur_time": "2024-08-18 15:04:10",
     * 		"run_time": "2024-08-18 15:04:12",
     * 		"run_jsmind_data": "{\"varpool_param\":\"[{\\\"varpool_domain\\\":\\\"domain1\\\",\\\"varpool_code\\\":\\\"name\\\"}]\",\"project_scene\":\"\",\"project_code\":\"\",\"type\":\"varpool\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"增加参数\",\"positionX\":271,\"rule_id\":\"1274686544979955712\",\"positionY\":205,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)增加参数\",\"more_task\":\"varpool\",\"id\":\"1274685896251150336\",\"data_status\":\"1\",\"divId\":\"1274685896251150336\",\"depend_level\":\"0\"}",
     * 		"next_tasks": "",
     * 		"pre_tasks": "1274745674495168513",
     * 		"is_disenable": "false",
     * 		"depend_level": "0",
     * 		"touch_type": "database",
     * 		"strategy_id": "1274685896251150336",
     * 		"group_type": "offline",
     * 		"data_node": "",
     * 		"is_notice": null
     *        }]
     * }
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public VarPoolCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
        getSftpUtil(this.dbConfig);
        initMinioClient(this.dbConfig);
    }

    @Override
    public boolean checkSftp() {
        return Boolean.valueOf(this.dbConfig.getOrDefault("sftp.enable", "false"));
    }

    @Override
    public String storageMode() {
        return this.dbConfig.getOrDefault("storage.mode", "");
    }

    @Override
    public String getBucket() {
        return this.dbConfig.getOrDefault("storage.minio.bucket", super.getBucket());
    }

    @Override
    public String getRegion() {
        return this.dbConfig.getOrDefault("storage.minio.region", super.getRegion());
    }

    @Override
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        initJinJavaCommonParam(strategyLogInfo, this.param);
        String logStr="";
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String varpool_params_str=run_jsmind_data.getOrDefault("varpool_param","").toString();

            Map<String, Object> commonParam = getJinJavaCommonParam();
            Jinjava jinjava = new Jinjava();
            varpool_params_str = jinjava.render(varpool_params_str, commonParam);

            List<Map> varpool_params = JSON.parseArray(varpool_params_str, Map.class);
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();

            if(is_disenable.equalsIgnoreCase("true")){

            }else{
                //写入变量池
                String key = "varpool:gid:"+strategyLogInfo.getStrategy_group_instance_id();
                for (Map varpool: varpool_params){
                    String varpool_code = varpool.getOrDefault("varpool_code","").toString();
                    String varpool_operate = varpool.getOrDefault("varpool_domain_operate","eq").toString();
                    String varpool_domain = varpool.getOrDefault("varpool_domain","").toString();
                    String varpool_value = varpool.getOrDefault("varpool_domain_value","").toString();
                    String varpool_domain_type = varpool.getOrDefault("varpool_domain_type","").toString();
                    String varpool_domain_sep = varpool.getOrDefault("varpool_domain_sep",";").toString();
                    String secondKey = varpool_domain+":"+varpool_code;

                    if(varpool_domain_type.equalsIgnoreCase("string")){
                        if(varpool_operate.equalsIgnoreCase("eq")){
                            JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                        }else if(varpool_operate.equalsIgnoreCase("add")){
                            Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                            Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                            if(value != null){
                                set = JSON.parseObject(value.toString(), Set.class);
                                set.add(varpool_value);
                            }
                            JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(set));
                        }else if(varpool_operate.equalsIgnoreCase("concat")){
                            Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                            if(value != null){
                                varpool_value = value.toString()+","+varpool_value;
                            }
                            JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                        }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                            throw new Exception("字符串类型,不支持KV追加操作");
                        }
                    }else if(varpool_domain_type.equalsIgnoreCase("int") || varpool_domain_type.equalsIgnoreCase("decimal")){
                        if(varpool_operate.equalsIgnoreCase("eq")){
                            JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
                        }else if(varpool_operate.equalsIgnoreCase("add")){
                            Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                            Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                            if(value != null){
                                Set set2 = JSON.parseObject(value.toString(), Set.class);
                                set2.add(set);
                                set = set2;
                            }
                            JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(set));
                        }else if(varpool_operate.equalsIgnoreCase("concat")){
                            throw new Exception("数值类型,不支持拼接操作");
                        }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                            throw new Exception("数值类型,不支持KV追加操作");
                        }
                    }else if(varpool_domain_type.equalsIgnoreCase("list")){
                        if(varpool_operate.equalsIgnoreCase("eq")){
                            List<Object> list = Lists.newArrayList(varpool_value.split(varpool_domain_sep));
                            JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(list));
                        }else if(varpool_operate.equalsIgnoreCase("add")){
                            Set set = Sets.newHashSet(varpool_value.split(varpool_domain_sep));
                            Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                            if(value != null){
                                Set set2 = JSON.parseObject(value.toString(), Set.class);
                                set2.addAll(set);
                                set = set2;
                            }
                            JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(set));
                        }else if(varpool_operate.equalsIgnoreCase("concat")){
                            throw new Exception("集合类型,不支持拼接操作");
                        }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                            throw new Exception("集合类型,不支持KV追加操作");
                        }
                    }else if(varpool_domain_type.equalsIgnoreCase("map")){
                        if(!varpool_value.contains("=")){
                            throw new Exception("字典格式 k1=v1;k2=v2");
                        }
                        if(varpool_operate.equalsIgnoreCase("eq")){
                            Map<String, String> map = parseKeyValuePairs(varpool_value, varpool_domain_sep);
                            JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(map));
                        }else if(varpool_operate.equalsIgnoreCase("add")){
                            throw new Exception("字典类型,请选择KV追加操作");
                        }else if(varpool_operate.equalsIgnoreCase("concat")){
                            throw new Exception("字典类型,不支持拼接操作");
                        }else if(varpool_operate.equalsIgnoreCase("kvadd")){
                            Map<String, String> map = parseKeyValuePairs(varpool_value, varpool_domain_sep);
                            Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);
                            if(value != null){
                                Map<String,String> old = JSON.parseObject(value.toString(), Map.class);
                                old.putAll(map);
                                map = old;
                            }
                            JedisPoolUtil.redisClient().hSet(key, secondKey, JSON.toJSONString(map));
                        }
                    }else{
                        throw new Exception("不支持的数据类型");
                    }
                }

                //缓存7天
                if(JedisPoolUtil.redisClient().isExists(key)){
                    JedisPoolUtil.redisClient().expire(key, 7*24*60*60L);
                }
            }

            Set<String> rs_error = Sets.difference(calculateResult.getRs(), rs);
            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin varpool run error: ", e);
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }

    public static Map<String, String> parseKeyValuePairs(String input, String sep) {
        Map<String, String> map = new HashMap<>();
        if (input != null && !input.isEmpty()) {
            String[] pairs = input.split(sep);
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    map.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return map;
    }

}
