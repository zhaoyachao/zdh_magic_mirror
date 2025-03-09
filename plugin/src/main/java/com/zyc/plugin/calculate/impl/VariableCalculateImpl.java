package com.zyc.plugin.calculate.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.DataPipe;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.groovy.GroovyFactory;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.VariableCalculate;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 变量池实现
 */
public class VariableCalculateImpl extends BaseCalculate implements VariableCalculate {
    private static Logger logger= LoggerFactory.getLogger(VariableCalculateImpl.class);

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

    public VariableCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            Set<DataPipe> rs_error = Sets.newHashSet();
            String file_dir = calculateResult.getFile_dir();

            if(is_disenable.equalsIgnoreCase("true")){

            }else{
                //写入变量池
                String key = "varpool:gid:"+strategyLogInfo.getStrategy_group_instance_id();
                String varpool_code = run_jsmind_data.getOrDefault("varpool_code","").toString();
                String varpool_operate = run_jsmind_data.getOrDefault("varpool_operate","eq").toString();
                String varpool_type = run_jsmind_data.getOrDefault("varpool_type","string").toString();
                String varpool_domain = run_jsmind_data.getOrDefault("varpool_domain","").toString();
                String varpool_value = run_jsmind_data.getOrDefault("varpool_value","").toString();
                String varpool_expre = run_jsmind_data.getOrDefault("varpool_expre","").toString();
                String secondKey = varpool_domain+":"+varpool_code;

                Map<String, Object> commonParam = getJinJavaCommonParam();
                Jinjava jinjava = new Jinjava();
                varpool_value = jinjava.render(varpool_value, commonParam);

                Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);

                if(value == null){
                    rs = Sets.newHashSet();
                }

                boolean ret = false;
                if(varpool_type.equalsIgnoreCase("string") || varpool_type.equalsIgnoreCase("decimal")){
                    ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
                }else if(varpool_type.equalsIgnoreCase("int") ){
                    ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
                }else if(varpool_type.equalsIgnoreCase("list")){
                    //获取变量表达式
                    if(!StringUtils.isEmpty(varpool_expre)){
                        Map<String, Object> parmas = new HashMap<>();
                        parmas.put("varpool_ret",  JSON.parseObject(value.toString(), List.class));
                        value = GroovyFactory.execExpress(varpool_expre, parmas);
                    }else{
                        value = JSON.parseObject(value.toString(), List.class);
                    }

                    if(value instanceof String){
                        ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
                    }else if(value instanceof Integer){
                        ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
                    }else if(value instanceof List){
                        ret = diffListValue((List)value,varpool_value, varpool_operate);
                    }else{
                        throw new Exception("不支持的数据类型");
                    }

                }else if(varpool_type.equalsIgnoreCase("map")){
                    //获取变量表达式
                    if(!StringUtils.isEmpty(varpool_expre)){
                        Map<String, Object> parmas = new HashMap<>();
                        parmas.put("varpool_ret",  JSON.parseObject(value.toString(), Map.class));
                        value = GroovyFactory.execExpress(varpool_expre, parmas);
                    }else{
                        value = JSON.parseObject(value.toString(), Map.class);
                    }

                    if(value instanceof String){
                        ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
                    }else if(value instanceof Integer){
                        ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
                    }else if(value instanceof Map){
                        ret = diffMapValue((Map)value,varpool_value, varpool_operate);
                    }else{
                        throw new Exception("不支持的数据类型");
                    }
                }

                if(ret == false){
                    rs_error = rs;
                    rs = Sets.newHashSet();
                }
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "变量对比结果:"+ret+", 实际值:"+value.toString()+" ,期望值:"+varpool_value);

            }

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin variable run error: ", e);
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }



    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffIntValue(Integer lValue, String uValue, String operate){
        try{

            if(operate.equalsIgnoreCase("gt")){
                if(lValue>Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue<Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue>=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue<=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.intValue() == Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(lValue.intValue() != Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffDoubleValue(Double lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.doubleValue() == Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.doubleValue() != Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffLongValue(Long lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.longValue() == Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.longValue() != Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffStringValue(String lValue, String uValue, String operate){
        try{
            int r = lValue.compareTo(uValue);
            if(operate.equalsIgnoreCase("gt")){
                if(r<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(r>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(r<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(r>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(r==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(r!=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                boolean in = Sets.newHashSet(uValue.split(";|,")).contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffListValue(List<Object> lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("in")){
                if(lValue.contains(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("like")){
                if(lValue.stream().filter(s->s.toString().contains(uValue)).collect(Collectors.toList()).size()>0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffMapValue(Map lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("in")){
                if(lValue.containsValue(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("like")){
                if(lValue.values().stream().filter(s->s.toString().contains(uValue)).count()>0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

}
